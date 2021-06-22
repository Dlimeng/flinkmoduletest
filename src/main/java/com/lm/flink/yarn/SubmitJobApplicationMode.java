package com.lm.flink.yarn;

import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.configuration.*;
import org.apache.flink.yarn.YarnClientYarnClusterInformationRetriever;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.flink.yarn.configuration.YarnDeploymentTarget;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.util.Collections;
import static org.apache.flink.configuration.JobManagerOptions.TOTAL_PROCESS_MEMORY;
import static org.apache.flink.configuration.MemorySize.MemoryUnit.MEGA_BYTES;
import  org.apache.hadoop.fs.FSOutputSummer;

public class SubmitJobApplicationMode {
    public static void main(String[] args) {
        String confDirctory="yarn-conf";
        String flinkLibs="libs/";
        String userJarPath="wordcount.jar";
        String flinkDistJar="flink-yarn";
        YarnClient yarnClient = YarnClient.createYarnClient();
        org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();

        configuration.addResource(new Path("yarn-site.xml"));
        configuration.addResource(new Path("hdfs-site.xml"));
        configuration.addResource(new Path("core-site.xml"));

        YarnConfiguration yarnConfiguration = new YarnConfiguration(configuration);
        yarnClient.init(yarnConfiguration);
        yarnClient.start();

        YarnClientYarnClusterInformationRetriever yarnClientYarnClusterInformationRetriever = YarnClientYarnClusterInformationRetriever.create(yarnClient);
        Configuration flinkConfiguration = GlobalConfiguration.loadConfiguration(confDirctory);
        flinkConfiguration.set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS,true);
        flinkConfiguration.set(PipelineOptions.JARS, Collections.singletonList(userJarPath));

        Path remoteLib = new Path(flinkLibs);
        flinkConfiguration.set(
                YarnConfigOptions.PROVIDED_LIB_DIRS,
                Collections.singletonList(remoteLib.toString()));

        flinkConfiguration.set(
                YarnConfigOptions.FLINK_DIST_JAR,
                flinkDistJar);


        flinkConfiguration.set(DeploymentOptions.TARGET, YarnDeploymentTarget.APPLICATION.getName());
        flinkConfiguration.set(JobManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.parse("1024",MEGA_BYTES));
        flinkConfiguration.set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.parse("1024",MEGA_BYTES));

        ClusterSpecification clusterSpecification = new ClusterSpecification.ClusterSpecificationBuilder()
                .createClusterSpecification();

        //设置用户jar的参数和主类
        ApplicationConfiguration appConfig = new ApplicationConfiguration(args, null);
        YarnClusterDescriptor yarnClusterDescriptor = new YarnClusterDescriptor(flinkConfiguration,yarnConfiguration,yarnClient,yarnClientYarnClusterInformationRetriever,true);

        ClusterClientProvider<ApplicationId> clusterClientProvider= null;
        try {
            clusterClientProvider = yarnClusterDescriptor.deployApplicationCluster(clusterSpecification,appConfig);
        }catch (Exception e){
            e.printStackTrace();
        }

        ClusterClient<ApplicationId> clusterClient = clusterClientProvider.getClusterClient();

    }
}
