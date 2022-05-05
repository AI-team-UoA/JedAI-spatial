package dit.anonymous.webapp.controllers.sparkControllers;

import SparkER.LivyJobs.ReadProfilesJob;
import org.apache.livy.LivyClient;
import org.apache.livy.LivyClientBuilder;
import org.json.JSONObject;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;

@RestController
@RequestMapping("/spark/dataread/**")
public class SparkDataReadController {

    @PostMapping(path = "/spark/dataread/setConfiguration")
    public String SetDataset(@RequestPart(required=true) String json_conf){
        try {
            JSONObject configurations = new JSONObject(json_conf);
            String filetype = configurations.getString("filetype");
            String source = configurations.has("filename") ? configurations.getString("filename") : configurations.getString("url");
            System.out.println(source);


            URI livyUrl = new URI("http://195.134.71.15:8998");
            URI SparkERJar = new URI("file:///usr/local/livy/apache-livy-0.7.0-incubating-bin/jars/spark_er-assembly-1.0.jar");
            LivyClient client = new LivyClientBuilder()
                    .setURI(livyUrl)
                    .build();
            client.addJar(SparkERJar).get();
            System.out.println("Local " + source);
            Object results = client.submit(new ReadProfilesJob(source)).get();
            return "";
        } catch (Exception e) {
            e.printStackTrace();
            return "";
        }
    }

}
