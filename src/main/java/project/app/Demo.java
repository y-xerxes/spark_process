package project.app;

import com.platform.data.framework.AbstractSparkApp;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.rdd.RDD;
import org.springframework.stereotype.Component;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
@Slf4j
public class Demo extends AbstractSparkApp {

    @Override
    public String appName() {
        return "demo-app";
    }

    @Override
    public void run(List<String> args) {
        log.info("batch任务starting");
        log.info("user.dir: {}", System.getProperty("user.dir"));
        log.info("app namespace: {}", this.joowingNamespace);
    }
}
