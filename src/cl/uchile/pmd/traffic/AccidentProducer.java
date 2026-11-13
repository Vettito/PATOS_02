package cl.uchile.pmd.traffic;

import java.io.BufferedReader;
import java.io.FileReader;
import java.text.SimpleDateFormat;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class AccidentProducer {
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Uso: AccidentProducer <csv_path> <topic> <speedup_factor>");
            System.exit(1);
        }

        String csvPath = args[0];
        String topic = args[1];
        double speedup = Double.parseDouble(args[2]); // Ej: 1000

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        try (BufferedReader br = new BufferedReader(new FileReader(csvPath))) {
            String header = br.readLine(); 
            String line;
            long prevTime = -1;

            System.out.println("Iniciando simulación de tráfico hacia Kafka...");

            while ((line = br.readLine()) != null) {
                // Regex segura para separar CSV con comillas internas
                String[] cols = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
                if (cols.length < 25) continue;

                try {
                    String id = cols[0];
                    int severity = Integer.parseInt(cols[1]);
                    // Parseo del Start_Time (asumiendo formato estándar YYYY-MM-DD HH:MM:SS)
                    String timeStr = cols[2].replace("\"", "").split("\\.")[0];
                    long currentTime = sdf.parse(timeStr).getTime();
                    double lat = Double.parseDouble(cols[5]);
                    double lng = Double.parseDouble(cols[6]);
                    String state = cols[13];
                    String weather = cols[24];

                    // Simulación de aceleración temporal (igual que en Lab 06)
                    if (prevTime != -1 && currentTime > prevTime) {
                        long delayMs = (long) ((currentTime - prevTime) / speedup);
                        if (delayMs > 0 && delayMs < 10000) {
                            Thread.sleep(delayMs);
                        }
                    }
                    prevTime = currentTime;

                    // Formato del mensaje a enviar
                    String payload = String.format("%s\t%d\t%d\t%.4f\t%.4f\t%s\t%s",
                            id, severity, currentTime, lat, lng, state, weather);

                    // Enviar usando 'state' como partition key para orden regional
                    producer.send(new ProducerRecord<>(topic, state, payload));

                } catch (Exception e) {
                    // Ignorar filas con formato corrupto o fechas inválidas
                    continue;
                }
            }
        }
        producer.close();
        System.out.println("Simulación de tráfico finalizada.");
    }
}

