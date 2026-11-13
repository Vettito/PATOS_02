package cl.uchile.pmd.traffic;

import java.time.Duration;
import java.util.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class SpatialBurstDetector {

    static class AccidentEvent {
        long timestamp;
        int severity;
        double lat, lng;
        String state;

        AccidentEvent(long t, int s, double lat, double lng, String state) {
            this.timestamp = t;
            this.severity = s;
            this.lat = lat;
            this.lng = lng;
            this.state = state;
        }
    }

    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println("Uso: SpatialBurstDetector <topic> <window_sec> <burst_threshold_x>");
            System.exit(1);
        }

        String topic = args[0];
        long windowMs = Long.parseLong(args[1]) * 1000; // Ventana temporal en milisegundos
        int xThreshold = Integer.parseInt(args[2]);      // Mínimo de accidentes graves para alerta

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "traffic-alert-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));

        Map<String, LinkedList<AccidentEvent>> spatialQueues = new HashMap<>();
        Map<String, Boolean> activeAlerts = new HashMap<>();

        System.out.println("SpatialBurstDetector escuchando alertas viales en tiempo real...");

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                String[] parts = record.value().split("\t");
                if (parts.length < 7) continue;

                try {
                    int severity = Integer.parseInt(parts[1]);
                    long timestamp = Long.parseLong(parts[2]);
                    double lat = Double.parseDouble(parts[3]);
                    double lng = Double.parseDouble(parts[4]);
                    String state = parts[5];

                    // Filtrar solo accidentes con severidad crítica (>= 3)
                    if (severity < 3) continue;

                    // Discretización espacial (Grilla de ~1.1 km)
                    String gridKey = String.format(Locale.US, "%.2f,%.2f", lat, lng);

                    spatialQueues.putIfAbsent(gridKey, new LinkedList<>());
                    LinkedList<AccidentEvent> queue = spatialQueues.get(gridKey);
                    queue.addLast(new AccidentEvent(timestamp, severity, lat, lng, state));

                    // Limpieza de la cola FIFO según la ventana de tiempo
                    while (!queue.isEmpty() && (timestamp - queue.getFirst().timestamp) > windowMs) {
                        queue.removeFirst();
                    }

                    // Lógica de detección de Burst (Basado en Lab 06)
                    boolean inEvent = activeAlerts.getOrDefault(gridKey, false);
                    if (!inEvent && queue.size() >= xThreshold) {
                        activeAlerts.put(gridKey, true);
                        System.out.printf(Locale.US, 
                            "[ALERTA DE RIESGO VIAL] Celda: %s (Estado: %s) | ¡%d accidentes graves detectados en la zona!%n",
                            gridKey, state, queue.size());
                    } else if (inEvent && queue.size() < (xThreshold / 2)) {
                        activeAlerts.put(gridKey, false);
                        System.out.printf("[FIN DE ALERTA] Retorno a la normalidad en celda: %s%n", gridKey);
                    }
                } catch (Exception e) {
                    continue;
                }
            }
        }
    }
}
