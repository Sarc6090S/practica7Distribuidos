package org.example;

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpExchange;
import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

class Info {
    int id, min, max;
    Info(int id, int min, int max) {
        this.id = id;
        this.min = min;
        this.max = max;
    }
}

class InfoPrimos implements Runnable {
    Info info;
    int[] resultados;
    StringBuilder primosList;

    InfoPrimos(Info info, int[] resultados, StringBuilder primosList) {
        this.info = info;
        this.resultados = resultados;
        this.primosList = primosList;
    }

    static boolean esPrimo(int n) {
        if (n == 0 || n == 1 || n == 4) return false;
        for (int i = 2; i < n / 2; i++)
            if (n % i == 0) return false;
        return true;
    }

    @Override
    public void run() {
        int suma = 0, total = 0;
        StringBuilder found = new StringBuilder("- Los numeros primos encontrados en el rango son: ");
        for (int i = info.min; i <= info.max; i++) {
            if (esPrimo(i)) {
                suma += i;
                total++;
                found.append(i).append(" ");
            }
        }
        synchronized (resultados) {
            resultados[0] += suma;
            resultados[1] += total;
            primosList.append("\n-------------------------------------------------\n");
            primosList.append("- Thread id: ").append(info.id).append("\n");
            primosList.append("- Limite inferior: ").append(info.min).append("\n");
            primosList.append("- Limite superior : ").append(info.max).append("\n");
            primosList.append("- \n");
            primosList.append(found).append("\n");
            primosList.append("- Sumatoria de todos los numeros primos encontrados: ").append(suma).append("\n");
            primosList.append("- Total de numeros primos encontrados en el rango: ").append(total).append("\n");
            primosList.append("- Total de numeros primos encontrados en total: ").append(resultados[1]).append("\n");
            primosList.append("-------------------------------------------------\n");
        }
    }
}

class DatosPar {
    String url;
    int id;
    String origen;
    DatosPar(String url, int id, String origen) {
        this.url = url;
        this.id = id;
        this.origen = origen;
    }
    DatosPar(String url, int id) {
        this(url, id, "inicial");
    }
}

class Cola {
    Queue<Object[]> fila = new LinkedList<>();
    int encolados = 0, atendidos = 0;
    String tag;

    Cola(String tag) { this.tag = tag; }

    synchronized void agregar(Object[] s) {
        fila.add(s);
        encolados++;
        System.out.println("[" + tag + " COLA] #" + s[0] + " encolada. Espera: " + fila.size());
        notifyAll();
    }

    synchronized Object[] siguiente() throws InterruptedException {
        while (fila.isEmpty()) wait();
        Object[] s = fila.poll();
        atendidos++;
        System.out.println("[" + tag + " COLA] #" + s[0] + " desencolada. Restantes: " + fila.size());
        return s;
    }

    synchronized int enEspera() { return fila.size(); }
    synchronized int totalAtendidos() { return atendidos; }

    synchronized String estado() {
        return "Espera: " + fila.size() + " | Atendidos: " + atendidos + "/" + encolados;
    }
}

class Despachador implements Runnable {
    Cola cola;
    int nodoId;
    Semaphore semaforo;
    CopyOnWriteArrayList<DatosPar> pares;
    AtomicInteger rrIndex;
    String miUrl;
    static AtomicInteger puertoAuto = new AtomicInteger(9000);
    static AtomicInteger idAuto = new AtomicInteger(100);
    static AtomicInteger nodosCreados = new AtomicInteger(0);
    static int maxNodos = 4;

    Despachador(Cola cola, int nodoId, Semaphore semaforo, CopyOnWriteArrayList<DatosPar> pares, AtomicInteger rrIndex, String miUrl) {
        this.cola = cola;
        this.nodoId = nodoId;
        this.semaforo = semaforo;
        this.pares = pares;
        this.rrIndex = rrIndex;
        this.miUrl = miUrl;
    }

    @Override
    public void run() {
        while (true) {
            try {
                Object[] datos = cola.siguiente();
                int id = (int) datos[0];
                int boundary = (int) datos[1];
                int threadss = (int) datos[2];
                String tipo = (String) datos[3];
                int origenId = (int) datos[4];
                String[] res = (String[]) datos[5];

                System.out.println("[NODO " + nodoId + " DESPACHADOR] #" + id + " | Semaforo: " + semaforo.availablePermits());

                new Thread(() -> {
                    String r;
                    if (tipo.equals("CLIENTE"))
                        r = atenderCliente(boundary, threadss, id);
                    else
                        r = atenderServicio(boundary, threadss, id, origenId);
                    synchronized (res) {
                        res[0] = r;
                        res.notifyAll();
                    }
                }).start();
            } catch (InterruptedException e) { break; }
        }
    }

    String atenderCliente(int boundary, int threadss, int solicitudId) {
        String t = "[NODO " + nodoId + "]";
        long ahora = System.currentTimeMillis();
        System.out.println("\n" + t + " ========== SOLICITUD #" + solicitudId + " (t=" + ahora + " thread=" + Thread.currentThread().getName() + ") ==========");
        System.out.println(t + " Limite: " + boundary + " | Hilos: " + threadss + " | Semaforo: " + semaforo.availablePermits());

        if (semaforo.tryAcquire()) {
            System.out.println(t + " #" + solicitudId + " ---> RECURSO OK, procesando LOCAL. Semaforo: " + semaforo.availablePermits());
            try {
                String resultado = hacerCalculo(boundary, threadss, t);
                return "{\"ok\":true,\"nodo\":" + nodoId + ",\"tipo\":\"local\"," + extraerJson(resultado) + "}";
            } finally {
                semaforo.release();
                System.out.println(t + " #" + solicitudId + " Liberado. Semaforo: " + semaforo.availablePermits());
            }
        }

        System.out.println(t + " #" + solicitudId + " *** SEMAFORO EN 0 ---> DELEGANDO/AUTO-ESCALANDO ***");
        String resultado = delegarAPar(boundary, threadss, t);
        System.out.println(t + " #" + solicitudId + " *** RESULTADO: " + (resultado.contains("autoEscalado") ? "AUTO-ESCALADO" : resultado.contains("fallback") ? "FALLBACK" : "DELEGADO") + " ***");
        return resultado;
    }

    String atenderServicio(int boundary, int threadss, int solicitudId, int origenId) {
        String t = "[NODO " + nodoId + "]";
        System.out.println("\n" + t + " ========== SERVICIO de NODO " + origenId + " #" + solicitudId + " ==========");

        try { semaforo.acquire(); } catch (InterruptedException e) {
            return "{\"ok\":false,\"error\":\"interrumpido\"}";
        }

        System.out.println(t + " Recurso para NODO " + origenId);
        try {
            String resultado = hacerCalculo(boundary, threadss, t);
            return "{\"ok\":true,\"nodo\":" + nodoId + ",\"tipo\":\"servicio\"," + extraerJson(resultado) + "}";
        } finally {
            semaforo.release();
            System.out.println(t + " Liberado tras servicio. Semaforo: " + semaforo.availablePermits());
        }
    }

    String intentarPares(int boundary, int threadss, String t) {
        int total = pares.size();
        for (int i = 0; i < total; i++) {
            int idx = rrIndex.getAndIncrement() % total;
            DatosPar par = pares.get(idx);
            System.out.println(t + " ---> Intentando NODO " + par.id + " (" + par.url + ")...");

            try {
                String body = "{\"limite\":" + boundary + ",\"hilos\":" + threadss + ",\"nodoOrigen\":" + nodoId + "}";
                String resp = httpPost(par.url + "/api/servicio", body);
                System.out.println(t + " <--- NODO " + par.id + " respondio OK");

                return resp.substring(0, resp.length() - 1)
                        + ",\"delegadoPor\":" + nodoId
                        + ",\"procesadoPor\":" + par.id
                        + ",\"esquema\":\"delegacion HTTP REST\"}";
            } catch (Exception e) {
                System.out.println(t + " NODO " + par.id + " no disponible: " + e.getMessage());
                if (i < total - 1) {
                    int sig = rrIndex.get() % total;
                    System.out.println(t + " Failover ---> NODO " + pares.get(sig).id);
                }
            }
        }
        return null;
    }

    String delegarAPar(int boundary, int threadss, String t) {
        String delegado = intentarPares(boundary, threadss, t);
        if (delegado != null) return delegado;

        System.out.println(t + " *** Ningun par disponible ---> intentando crear nodo nuevo ***");
        DatosPar nuevo = spawnNodo(t);

        if (nuevo == null) {
            for (int espera = 0; espera < 10 && pares.isEmpty(); espera++) {
                try { Thread.sleep(300); } catch (InterruptedException e) { break; }
            }
            System.out.println(t + " ---> Reintentando con los nodos que ya existen (" + pares.size() + ")");
            delegado = intentarPares(boundary, threadss, t);
            if (delegado != null) return delegado;
        }

        if (nuevo != null) {
            System.out.println(t + " ---> Nodo " + nuevo.id + " creado en " + nuevo.url);
            try {
                String body = "{\"limite\":" + boundary + ",\"hilos\":" + threadss + ",\"nodoOrigen\":" + nodoId + "}";
                String resp = httpPost(nuevo.url + "/api/servicio", body);
                System.out.println(t + " <--- Nodo " + nuevo.id + " respondio OK (auto-escalado)");
                return resp.substring(0, resp.length() - 1)
                        + ",\"delegadoPor\":" + nodoId
                        + ",\"procesadoPor\":" + nuevo.id
                        + ",\"autoEscalado\":true"
                        + ",\"esquema\":\"auto-escalado HTTP REST\"}";
            } catch (Exception e2) {
                System.out.println(t + " Nodo nuevo " + nuevo.id + " fallo: " + e2.getMessage());
            }
        }

        System.out.println(t + " Fallback ---> esperando recurso local...");
        try { semaforo.acquire(); } catch (InterruptedException e) {
            return "{\"ok\":false,\"error\":\"interrumpido\"}";
        }
        try {
            String resultado = hacerCalculo(boundary, threadss, t);
            return "{\"ok\":true,\"nodo\":" + nodoId + ",\"tipo\":\"fallback_local\"," + extraerJson(resultado) + "}";
        } finally {
            semaforo.release();
        }
    }

    DatosPar spawnNodo(String t) {
        if (nodosCreados.incrementAndGet() > maxNodos) {
            nodosCreados.decrementAndGet();
            System.out.println(t + " >>> TOPE DE NODOS ALCANZADO (" + maxNodos + ") ---> no creo mas");
            return null;
        }

        int nuevoId = idAuto.getAndIncrement();
        int nuevoPuerto = 0;

        for (int intento = 0; intento < 30; intento++) {
            int candidato = puertoAuto.getAndIncrement();
            System.out.println(t + " >>> SPAWN: creando NODO " + nuevoId + " en puerto " + candidato + "...");
            try {
                NodoWeb.levantarNodo(candidato, nuevoId);
                nuevoPuerto = candidato;
                break;
            } catch (BindException be) {
                System.out.println(t + " >>> Puerto " + candidato + " ocupado ---> probando el siguiente");
            } catch (Exception e) {
                System.out.println(t + " >>> SPAWN ERROR: " + e.getClass().getName() + ": " + e.getMessage());
                nodosCreados.decrementAndGet();
                return null;
            }
        }

        if (nuevoPuerto == 0) {
            System.out.println(t + " >>> SPAWN ERROR: no encontre ningun puerto libre");
            nodosCreados.decrementAndGet();
            return null;
        }

        String nuevoUrl = "http://localhost:" + nuevoPuerto;
        System.out.println(t + " >>> SPAWN OK: NODO " + nuevoId + " LISTO en " + nuevoUrl);

        pares.add(new DatosPar(nuevoUrl, nuevoId, "auto"));
        System.out.println(t + " >>> NODO " + nuevoId + " agregado a pares. Total pares: " + pares.size());

        return new DatosPar(nuevoUrl, nuevoId, "auto");
    }

    String hacerCalculo(int boundary, int threadss, String t) {
        if (boundary <= 1) return "ERROR:NO SE ACEPTAN NUMEROS NEGATIVOS O 1";
        if (threadss < 1) return "ERROR:NO SE ACEPTA MENOS DE 1 HILO";
        if (threadss > boundary) return "ERROR:MAS HILOS QUE NUMEROS (" + threadss + ">" + boundary + ")";

        System.out.println(t + " Procesando... (Thread.sleep 3s)");
        try { Thread.sleep(3000); } catch (InterruptedException e) { }

        int[] resultados = new int[2];
        StringBuilder primosList = new StringBuilder();
        int rango = boundary / threadss;
        Thread[] hilos = new Thread[threadss];
        int inicio = 1;

        for (int i = 0; i < threadss; i++) {
            int min = inicio;
            int max;
            inicio += rango - 1;
            if (i == threadss - 1 && inicio < boundary) max = boundary;
            else if (inicio > boundary) max = boundary;
            else max = inicio;
            inicio++;
            hilos[i] = new Thread(new InfoPrimos(new Info(i, min, max), resultados, primosList));
            hilos[i].start();
        }

        for (int i = 0; i < threadss; i++) {
            try { hilos[i].join(); } catch (InterruptedException e) { }
        }

        System.out.println(t + " Primos: " + resultados[1] + " | Suma: " + resultados[0]);
        return resultados[0] + "|" + resultados[1] + "|" + primosList.toString();
    }

    String extraerJson(String calc) {
        if (calc.startsWith("ERROR:"))
            return "\"ok\":false,\"error\":\"" + escJson(calc.substring(6)) + "\"";
        String[] partes = calc.split("\\|", 3);
        return "\"suma\":" + partes[0] + ",\"totalPrimos\":" + partes[1]
                + ",\"detalle\":\"" + escJson(partes[2]) + "\"";
    }

    static String escJson(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"")
                .replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t");
    }

    static String httpGet(String urlStr, int timeout) {
        try {
            URL url = new URL(urlStr);
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod("GET");
            con.setConnectTimeout(timeout);
            con.setReadTimeout(timeout);
            int code = con.getResponseCode();
            if (code >= 400) { con.disconnect(); return null; }
            BufferedReader br = new BufferedReader(new InputStreamReader(con.getInputStream(), StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            String linea;
            while ((linea = br.readLine()) != null) sb.append(linea);
            br.close();
            con.disconnect();
            return sb.toString();
        } catch (Exception e) {
            return null;
        }
    }

    static String httpPost(String urlStr, String body) throws Exception {
        URL url = new URL(urlStr);
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setRequestMethod("POST");
        con.setRequestProperty("Content-Type", "application/json");
        con.setDoOutput(true);
        con.setConnectTimeout(5000);
        con.setReadTimeout(120000);
        try (OutputStream os = con.getOutputStream()) {
            os.write(body.getBytes(StandardCharsets.UTF_8));
        }
        int code = con.getResponseCode();
        InputStream is = (code >= 200 && code < 300) ? con.getInputStream() : con.getErrorStream();
        BufferedReader br = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
        StringBuilder sb = new StringBuilder();
        String linea;
        while ((linea = br.readLine()) != null) sb.append(linea);
        br.close();
        con.disconnect();
        if (code >= 400) throw new IOException("HTTP " + code + ": " + sb);
        return sb.toString();
    }
}

public class NodoWeb {
    static int nodoId;
    static Cola cola;
    static Semaphore semaforo;
    static AtomicInteger contador = new AtomicInteger(0);
    static CopyOnWriteArrayList<DatosPar> pares = new CopyOnWriteArrayList<>();
    static String miUrl = "";
    static int puertoBase;
    static Map<String, byte[]> archivosCache = new HashMap<>();

    public static void main(String[] args) throws Exception {
        int puerto = Integer.parseInt(getEnvOrArg(args, 0, "PORT", "8080"));
        nodoId = Integer.parseInt(getEnvOrArg(args, 1, "NODO_ID", "1"));
        String paresStr = getEnvOrArg(args, 2, "PARES", "");
        miUrl = getEnvOrArg(args, 3, "MI_URL", "");
        puertoBase = puerto;

        if (!paresStr.isEmpty()) {
            for (String p : paresStr.split(",")) {
                String[] parts = p.split("\\|");
                pares.add(new DatosPar(parts[0].trim(), Integer.parseInt(parts[1].trim())));
            }
        }

        int maxId = nodoId;
        for (DatosPar par : pares)
            if (par.id > maxId) maxId = par.id;

        Despachador.puertoAuto.set(puerto + 1);
        Despachador.idAuto.set(maxId + 1);
        Despachador.maxNodos = Integer.parseInt(getEnvOrArg(args, 4, "MAX_NODOS", "4"));

        semaforo = new Semaphore(1, true);
        cola = new Cola("NODO " + nodoId);
        AtomicInteger rrIndex = new AtomicInteger(0);

        System.out.println("*****************************************************************");
        System.out.println("*****   Practica 7 - PWA (Aplicacion Web Progresiva)       ******");
        System.out.println("*****   NODO " + nodoId + " - Puerto " + puerto + "                              *****");
        System.out.println("*****  Autor: Moreno Zarate Israel            Grupo: 7CM1   *****");
        System.out.println("*****************************************************************\n");

        System.out.println("[NODO " + nodoId + "] Puerto HTTP: " + puerto);
        System.out.println("[NODO " + nodoId + "] URL propia: " + (miUrl.isEmpty() ? "(no definida)" : miUrl));
        System.out.println("[NODO " + nodoId + "] Semaforo: 1 recurso");
        System.out.println("[NODO " + nodoId + "] Cola FIFO + Despachador");
        System.out.println("[NODO " + nodoId + "] Tope de nodos auto-escalados: " + Despachador.maxNodos);
        System.out.println("[NODO " + nodoId + "] Pares iniciales (" + pares.size() + "):");
        for (DatosPar par : pares)
            System.out.println("    ---> NODO " + par.id + ": " + par.url);

        Thread desp = new Thread(new Despachador(cola, nodoId, semaforo, pares, rrIndex, miUrl));
        desp.setDaemon(true);
        desp.start();

        registrarConPares();

        HttpServer server = HttpServer.create(new InetSocketAddress(puerto), 0);

        server.createContext("/", ex -> {
            String path = ex.getRequestURI().getPath();
            if (path.equals("/") || path.equals("/index.html")) {
                servirArchivo(ex, "index.html", "text/html; charset=utf-8");
            } else if (path.equals("/manifest.json")) {
                servirArchivo(ex, "manifest.json", "application/json; charset=utf-8");
            } else if (path.equals("/sw.js")) {
                servirArchivo(ex, "sw.js", "application/javascript; charset=utf-8");
            } else {
                String resp = "{\"error\":\"no encontrado\"}";
                enviarJson(ex, 404, resp);
            }
        });

        server.createContext("/api/primos", ex -> {
            corsHeaders(ex);
            ex.getResponseHeaders().set("Connection", "close");
            if (ex.getRequestMethod().equals("OPTIONS")) { ex.sendResponseHeaders(204, -1); ex.close(); return; }

            Map<String, String> params = parseQuery(ex.getRequestURI().getQuery());
            int limite, hilos;
            try {
                limite = Integer.parseInt(params.getOrDefault("limite", "0"));
                hilos = Integer.parseInt(params.getOrDefault("hilos", "1"));
            } catch (NumberFormatException e) {
                enviarJson(ex, 400, "{\"ok\":false,\"error\":\"parametros invalidos\"}");
                return;
            }

            if (limite <= 0 || hilos <= 0) {
                enviarJson(ex, 400, "{\"ok\":false,\"error\":\"limite e hilos deben ser positivos\"}");
                return;
            }

            int id = contador.incrementAndGet();
            long tEntrada = System.currentTimeMillis();
            System.out.println("\n[NODO " + nodoId + "] GET /api/primos?limite=" + limite + "&hilos=" + hilos + " ---> #" + id + " (t=" + tEntrada + ")");

            String[] res = {null};
            cola.agregar(new Object[]{id, limite, hilos, "CLIENTE", 0, res});

            synchronized (res) {
                while (res[0] == null) {
                    try { res.wait(); } catch (InterruptedException e) { break; }
                }
            }

            long tSalida = System.currentTimeMillis();
            System.out.println("[NODO " + nodoId + "] #" + id + " respondido en " + (tSalida - tEntrada) + "ms");
            enviarJson(ex, 200, res[0]);
        });

        server.createContext("/api/servicio", ex -> {
            corsHeaders(ex);
            if (ex.getRequestMethod().equals("OPTIONS")) {
                ex.getResponseHeaders().set("Access-Control-Allow-Headers", "Content-Type");
                ex.sendResponseHeaders(204, -1); ex.close(); return;
            }

            String body = new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            int limite = extraerInt(body, "limite");
            int hilos = extraerInt(body, "hilos");
            int origen = extraerInt(body, "nodoOrigen");

            int id = contador.incrementAndGet();
            System.out.println("\n[NODO " + nodoId + "] <--- POST /api/servicio de NODO " + origen + " ---> #" + id);

            String[] res = {null};
            cola.agregar(new Object[]{id, limite, hilos, "SERVICIO", origen, res});

            synchronized (res) {
                while (res[0] == null) {
                    try { res.wait(); } catch (InterruptedException e) { break; }
                }
            }

            enviarJson(ex, 200, res[0]);
        });

        server.createContext("/api/registrar", ex -> {
            corsHeaders(ex);
            if (ex.getRequestMethod().equals("OPTIONS")) {
                ex.getResponseHeaders().set("Access-Control-Allow-Headers", "Content-Type");
                ex.sendResponseHeaders(204, -1); ex.close(); return;
            }

            String body = new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            int idNuevo = extraerInt(body, "nodoId");
            String urlNuevo = extraerStr(body, "url");

            if (urlNuevo.isEmpty() || idNuevo == 0) {
                enviarJson(ex, 400, "{\"ok\":false,\"error\":\"falta nodoId o url\"}");
                return;
            }

            boolean yaExiste = false;
            for (DatosPar p : pares) {
                if (p.id == idNuevo || p.url.equals(urlNuevo)) { yaExiste = true; break; }
            }

            if (!yaExiste) {
                pares.add(new DatosPar(urlNuevo, idNuevo, "auto"));
                System.out.println("[NODO " + nodoId + "] <--- REGISTRO: Nodo " + idNuevo + " se registro (" + urlNuevo + ")");
                System.out.println("[NODO " + nodoId + "] Pares ahora: " + pares.size());
            } else {
                System.out.println("[NODO " + nodoId + "] Nodo " + idNuevo + " ya estaba registrado");
            }

            StringBuilder paresJson = new StringBuilder("[");
            for (int i = 0; i < pares.size(); i++) {
                DatosPar p = pares.get(i);
                if (i > 0) paresJson.append(",");
                paresJson.append("{\"id\":").append(p.id)
                        .append(",\"url\":\"").append(p.url).append("\"")
                        .append(",\"origen\":\"").append(p.origen).append("\"}");
            }
            paresJson.append("]");

            enviarJson(ex, 200, "{\"ok\":true,\"nodo\":" + nodoId + ",\"mensaje\":\"registrado\",\"pares\":" + paresJson + "}");
        });

        server.createContext("/api/nodos", ex -> {
            corsHeaders(ex);
            if (ex.getRequestMethod().equals("OPTIONS")) { ex.sendResponseHeaders(204, -1); ex.close(); return; }

            StringBuilder json = new StringBuilder("{\"nodo\":" + nodoId);
            json.append(",\"url\":\"").append(Despachador.escJson(miUrl)).append("\"");
            json.append(",\"semaforo\":").append(semaforo.availablePermits());
            json.append(",\"colaEspera\":").append(cola.enEspera());
            json.append(",\"atendidos\":").append(cola.totalAtendidos());
            json.append(",\"maxNodos\":").append(Despachador.maxNodos);
            json.append(",\"nodosCreados\":").append(Despachador.nodosCreados.get());
            json.append(",\"pares\":[");

            for (int i = 0; i < pares.size(); i++) {
                DatosPar p = pares.get(i);
                if (i > 0) json.append(",");
                json.append("{\"id\":").append(p.id)
                        .append(",\"url\":\"").append(Despachador.escJson(p.url)).append("\"")
                        .append(",\"origen\":\"").append(p.origen).append("\"");

                if (esInterno(p.url)) {
                    json.append(",\"interno\":true");
                    String estado = Despachador.httpGet(p.url + "/api/estado", 1500);
                    if (estado != null) {
                        json.append(",\"online\":true")
                                .append(",\"semaforo\":").append(extraerInt(estado, "semaforo"))
                                .append(",\"colaEspera\":").append(extraerInt(estado, "colaEspera"))
                                .append(",\"atendidos\":").append(extraerInt(estado, "atendidos"));
                    } else {
                        json.append(",\"online\":false");
                    }
                }
                json.append("}");
            }
            json.append("]}");

            enviarJson(ex, 200, json.toString());
        });

        server.createContext("/api/lote", ex -> {
            corsHeaders(ex);
            ex.getResponseHeaders().set("Connection", "close");
            if (ex.getRequestMethod().equals("OPTIONS")) {
                ex.getResponseHeaders().set("Access-Control-Allow-Headers", "Content-Type");
                ex.sendResponseHeaders(204, -1); ex.close(); return;
            }

            String body = new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            int limite = extraerInt(body, "limite");
            int hilos = extraerInt(body, "hilos");
            int cantidad = extraerInt(body, "cantidad");
            if (cantidad < 1) cantidad = 5;
            if (cantidad > 20) cantidad = 20;

            System.out.println("\n[NODO " + nodoId + "] POST /api/lote cantidad=" + cantidad + " limite=" + limite + " hilos=" + hilos);

            String[][] resultados = new String[cantidad][1];
            int[] ids = new int[cantidad];

            for (int i = 0; i < cantidad; i++) {
                ids[i] = contador.incrementAndGet();
                cola.agregar(new Object[]{ids[i], limite, hilos, "CLIENTE", 0, resultados[i]});
            }

            for (int i = 0; i < cantidad; i++) {
                synchronized (resultados[i]) {
                    while (resultados[i][0] == null) {
                        try { resultados[i].wait(); } catch (InterruptedException e) { break; }
                    }
                }
            }

            StringBuilder json = new StringBuilder("[");
            for (int i = 0; i < cantidad; i++) {
                if (i > 0) json.append(",");
                json.append(resultados[i][0]);
            }
            json.append("]");

            System.out.println("[NODO " + nodoId + "] Lote de " + cantidad + " completado");
            enviarJson(ex, 200, json.toString());
        });

        server.createContext("/api/estado", ex -> {
            corsHeaders(ex);
            String json = "{\"nodo\":" + nodoId
                    + ",\"semaforo\":" + semaforo.availablePermits()
                    + ",\"cola\":\"" + Despachador.escJson(cola.estado()) + "\""
                    + ",\"colaEspera\":" + cola.enEspera()
                    + ",\"atendidos\":" + cola.totalAtendidos()
                    + ",\"pares\":" + pares.size() + "}";
            enviarJson(ex, 200, json);
        });

        server.setExecutor(java.util.concurrent.Executors.newFixedThreadPool(20));
        server.start();

        System.out.println("\n[NODO " + nodoId + "] Servidor HTTP listo en puerto " + puerto);
        System.out.println("[NODO " + nodoId + "] Endpoints:");
        System.out.println("    GET  /                              ---> Pagina PWA");
        System.out.println("    GET  /api/primos?limite=N&hilos=N   ---> Calcular primos");
        System.out.println("    POST /api/servicio                  ---> Servicio inter-nodo");
        System.out.println("    POST /api/registrar                 ---> Registrar nodo nuevo");
        System.out.println("    GET  /api/nodos                     ---> Lista de nodos");
        System.out.println("    POST /api/lote                      ---> Lote de solicitudes paralelas");
        System.out.println("    GET  /api/estado                    ---> Estado del nodo");
        System.out.println("    GET  /manifest.json                 ---> Manifest PWA");
        System.out.println("    GET  /sw.js                         ---> ServiceWorker\n");
    }

    static void levantarNodo(int puerto, int id) throws Exception {
        HttpServer srv = HttpServer.create(new InetSocketAddress(puerto), 0);

        String urlPropia = "http://localhost:" + puerto;
        Semaphore sem = new Semaphore(1, true);
        Cola c = new Cola("NODO " + id);
        AtomicInteger rr = new AtomicInteger(0);
        CopyOnWriteArrayList<DatosPar> p = new CopyOnWriteArrayList<>();
        AtomicInteger cnt = new AtomicInteger(0);

        Thread d = new Thread(new Despachador(c, id, sem, p, rr, urlPropia));
        d.setDaemon(true);
        d.start();

        srv.createContext("/api/servicio", ex -> {
            corsHeaders(ex);
            if (ex.getRequestMethod().equals("OPTIONS")) {
                ex.getResponseHeaders().set("Access-Control-Allow-Headers", "Content-Type");
                ex.sendResponseHeaders(204, -1); ex.close(); return;
            }
            String body = new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            int limite = extraerInt(body, "limite");
            int hilos = extraerInt(body, "hilos");
            int origen = extraerInt(body, "nodoOrigen");
            int sid = cnt.incrementAndGet();
            System.out.println("\n[NODO " + id + "] <--- POST /api/servicio de NODO " + origen + " ---> #" + sid);
            String[] res = {null};
            c.agregar(new Object[]{sid, limite, hilos, "SERVICIO", origen, res});
            synchronized (res) {
                while (res[0] == null) {
                    try { res.wait(); } catch (InterruptedException e) { break; }
                }
            }
            enviarJson(ex, 200, res[0]);
        });

        srv.createContext("/api/estado", ex -> {
            corsHeaders(ex);
            String json = "{\"nodo\":" + id
                    + ",\"url\":\"" + urlPropia + "\""
                    + ",\"semaforo\":" + sem.availablePermits()
                    + ",\"cola\":\"" + Despachador.escJson(c.estado()) + "\""
                    + ",\"colaEspera\":" + c.enEspera()
                    + ",\"atendidos\":" + c.totalAtendidos()
                    + ",\"pares\":" + p.size()
                    + ",\"origen\":\"auto\"}";
            enviarJson(ex, 200, json);
        });

        srv.createContext("/api/nodos", ex -> {
            corsHeaders(ex);
            if (ex.getRequestMethod().equals("OPTIONS")) { ex.sendResponseHeaders(204, -1); ex.close(); return; }

            StringBuilder json = new StringBuilder("{\"nodo\":" + id);
            json.append(",\"url\":\"").append(urlPropia).append("\"");
            json.append(",\"semaforo\":").append(sem.availablePermits());
            json.append(",\"colaEspera\":").append(c.enEspera());
            json.append(",\"atendidos\":").append(c.totalAtendidos());
            json.append(",\"origen\":\"auto\"");
            json.append(",\"pares\":[");

            for (int i = 0; i < p.size(); i++) {
                DatosPar dp = p.get(i);
                if (i > 0) json.append(",");
                json.append("{\"id\":").append(dp.id)
                        .append(",\"url\":\"").append(Despachador.escJson(dp.url)).append("\"")
                        .append(",\"origen\":\"").append(dp.origen).append("\"}");
            }
            json.append("]}");

            enviarJson(ex, 200, json.toString());
        });

        srv.setExecutor(java.util.concurrent.Executors.newFixedThreadPool(5));
        srv.start();
        System.out.println("[NODO " + id + "] Nodo auto-escalado listo en puerto " + puerto);
    }

    static void registrarConPares() {
        if (miUrl.isEmpty()) {
            System.out.println("[NODO " + nodoId + "] Sin MI_URL, no me puedo auto-registrar con pares");
            return;
        }
        for (DatosPar par : pares) {
            new Thread(() -> {
                try {
                    String body = "{\"nodoId\":" + nodoId + ",\"url\":\"" + miUrl + "\"}";
                    String resp = Despachador.httpPost(par.url + "/api/registrar", body);
                    System.out.println("[NODO " + nodoId + "] ---> Registrado con NODO " + par.id + ": " + resp);
                } catch (Exception e) {
                    System.out.println("[NODO " + nodoId + "] No pude registrarme con NODO " + par.id + ": " + e.getMessage());
                }
            }).start();
        }
    }

    static boolean esInterno(String url) {
        return url.contains("localhost") || url.contains("127.0.0.1");
    }

    static void corsHeaders(HttpExchange ex) {
        ex.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
        ex.getResponseHeaders().set("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
    }

    static void servirArchivo(HttpExchange ex, String nombre, String contentType) throws IOException {
        byte[] data = archivosCache.get(nombre);
        if (data == null) {
            try (InputStream is = NodoWeb.class.getResourceAsStream("/" + nombre)) {
                if (is != null) { data = is.readAllBytes(); }
            } catch (Exception e) { }
            if (data == null) {
                try {
                    java.nio.file.Path p = java.nio.file.Path.of(nombre);
                    if (java.nio.file.Files.exists(p)) data = java.nio.file.Files.readAllBytes(p);
                } catch (Exception e) { }
            }
            if (data == null) {
                enviarJson(ex, 404, "{\"error\":\"" + nombre + " no encontrado\"}");
                return;
            }
            if (!nombre.equals("sw.js")) archivosCache.put(nombre, data);
        }
        ex.getResponseHeaders().set("Content-Type", contentType);
        if (nombre.equals("sw.js"))
            ex.getResponseHeaders().set("Cache-Control", "no-cache");
        ex.sendResponseHeaders(200, data.length);
        ex.getResponseBody().write(data);
        ex.getResponseBody().close();
    }

    static void enviarJson(HttpExchange ex, int code, String json) throws IOException {
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        ex.sendResponseHeaders(code, bytes.length);
        ex.getResponseBody().write(bytes);
        ex.getResponseBody().close();
    }

    static Map<String, String> parseQuery(String query) {
        Map<String, String> map = new HashMap<>();
        if (query == null || query.isEmpty()) return map;
        for (String pair : query.split("&")) {
            String[] kv = pair.split("=", 2);
            if (kv.length == 2) map.put(kv[0], kv[1]);
        }
        return map;
    }

    static int extraerInt(String json, String key) {
        String buscar = "\"" + key + "\":";
        int idx = json.indexOf(buscar);
        if (idx < 0) return 0;
        idx += buscar.length();
        StringBuilder num = new StringBuilder();
        while (idx < json.length() && (Character.isDigit(json.charAt(idx)) || json.charAt(idx) == '-')) {
            num.append(json.charAt(idx));
            idx++;
        }
        return Integer.parseInt(num.toString());
    }

    static String extraerStr(String json, String key) {
        String buscar = "\"" + key + "\":\"";
        int idx = json.indexOf(buscar);
        if (idx < 0) return "";
        idx += buscar.length();
        int fin = json.indexOf("\"", idx);
        if (fin < 0) return "";
        return json.substring(idx, fin);
    }

    static String getEnvOrArg(String[] args, int idx, String envKey, String defaultVal) {
        String env = System.getenv(envKey);
        if (env != null && !env.isEmpty()) return env;
        if (args.length > idx) return args[idx];
        return defaultVal;
    }
}
