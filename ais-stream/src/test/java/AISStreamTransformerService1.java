import com.google.gson.*;
import com.kafka.stream.service.CustomValueMapper;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.deeplearning4j.nn.modelimport.keras.KerasModelImport;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;
import org.nd4j.linalg.io.ClassPathResource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service
public class AISStreamTransformerService1 {

    @Value("${kafka.topic.ais-input}")
    private String inputTopic;

    @Value("${kafka.topic.ais-output}")
    private String outputTopic;

    @Value("${model.repo-path}")
    private String modelPath;

    @Autowired
    private MultiLayerNetwork model;

    @Bean
    public KStream<String, String> kStream(StreamsBuilder kStreamBuilder) {
        KStream<String, String> stream = kStreamBuilder.stream(inputTopic);
        stream.filter((key, value) -> {
            INDArray input = Nd4j.create(preparefeatureVector(value));
            INDArray output = model.output(input);
            String prediction = output.toString();
            double prob = output.getRow(0).getDouble(0);
            String r = prob != 1 ? ANOMALY_FLAG : NORMAL_FLAG;
            return r.equals(ANOMALY_FLAG);
        }).mapValues(new CustomValueMapper(ANOMALY_FLAG)).to(outputTopic);
        return stream;
    }

    private double [][] preparefeatureVector(String json) {
        List<Double> featureList = new ArrayList();
        JsonElement root = JsonParser.parseString(json);
        JsonObject jsonObj = (JsonObject) root.getAsJsonObject();
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        for (Map.Entry<String, JsonElement> entry : jsonObj.entrySet()) {
            if (rset.contains(entry.getKey())) continue;
            String value = StringUtils.defaultString(gson.fromJson(entry.getValue(), String.class)).trim();
            featureList.add(Double.valueOf("".equals(value) ? "0" : value).doubleValue());
        }
        featureList.add(new Random().nextDouble());
        featureList.add(new Random().nextDouble());
        featureList.add(new Random().nextDouble());
        featureList.add(new Random().nextDouble());
        featureList.add(new Random().nextDouble());

        double [][] fVector = new double [1][featureList.size()];
        int counter = 0;
        for(Double d : featureList ) {
            fVector[0][counter++] = d;
        }
        return fVector;
    }

    @Bean
    public MultiLayerNetwork loadModel() throws Exception {
        String simpleMlp = new ClassPathResource(modelPath).getFile().getPath();
        System.out.println(simpleMlp.toString());
        MultiLayerNetwork model = KerasModelImport.importKerasSequentialModelAndWeights(simpleMlp);
        return model;
    }

    private static final String ANOMALY_FLAG = "Anomaly";
    private static final String NORMAL_FLAG = "Normal";

    private static final Set<String> rset = Stream.of("BaseDateTime",
                                                    "VesselName",
                                                    "IMO",
                                                    "CallSign",
                                                    "Status",
                                                    "timeDiff").collect(Collectors.toCollection(HashSet::new));
}
