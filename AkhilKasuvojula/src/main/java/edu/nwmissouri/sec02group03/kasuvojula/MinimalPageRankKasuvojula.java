package edu.nwmissouri.sec02group03.kasuvojula;

import java.util.Arrays;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

public class MinimalPageRankKasuvojula {

  public static void main(String[] args) {

    PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline p = Pipeline.create(options);
    String dataFolder = "web04";
   
   
    PCollection<KV<String,String>> k1 = AkhilMapper01(p,dataFolder,"go.md");
    PCollection<KV<String,String>> k2 = AkhilMapper01(p,dataFolder,"python.md");
    PCollection<KV<String,String>> k3 = AkhilMapper01(p,dataFolder,"java.md");
    PCollection<KV<String,String>> k4 = AkhilMapper01(p,dataFolder,"README.md");
 
    
     PCollectionList<KV<String, String>> pCollectionList = PCollectionList.of(k1).and(k2).and(k3).and(k4);
    PCollection<KV<String, String>> mergedList = pCollectionList.apply(Flatten.<KV<String, String>>pCollections());
    PCollection<KV<String, Iterable<String>>> urlToDocs = mergedList.apply(GroupByKey.<String, String>create());

    PCollection<String> pLinksStr = urlToDocs.apply(
        MapElements.into(
            TypeDescriptors.strings())
            .via((mergeOut) -> mergeOut.toString()));

    pLinksStr.apply(TextIO.write().to("Kasuvojulaout"));

    p.run().waitUntilFinish();
  }

  private static PCollection<KV<String, String>> AkhilMapper01(Pipeline p, String dataFolder, String dataFile) {
    String dataLocation = dataFolder + "/" + dataFile;
    PCollection<String> pcolInputLines = p.apply(TextIO.read().from(dataLocation));

    PCollection<String> pcolLinkLines = pcolInputLines.apply(Filter.by((String line) -> line.startsWith("[")));
    PCollection<String> pcolLinkPages = pcolLinkLines.apply(MapElements.into(TypeDescriptors.strings())
        .via(
            (String linkline) -> linkline.substring(linkline.indexOf("(") + 1, linkline.length() - 1)));
    PCollection<KV<String, String>> pcolKVpairs = pcolLinkPages.apply(MapElements
        .into(
            TypeDescriptors.kvs(
                TypeDescriptors.strings(), TypeDescriptors.strings()))
        .via(outlink -> KV.of(dataFile, outlink)));
    return pcolKVpairs;

  }
}
