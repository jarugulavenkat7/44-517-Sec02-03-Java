package edu.nwmissouri.sec02group03.jarugula;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

public class MinimalPageRankJarugula {


  static class Job1Finalizer extends DoFn<KV<String, Iterable<String>>, KV<String, RankedPage>> {
    
    @ProcessElement
    public void processElement(@Element KV<String, Iterable<String>> element,
        OutputReceiver<KV<String, RankedPage>> receiver) {
      Integer contributorVotes = 0;
      if (element.getValue() instanceof Collection) {
        contributorVotes = ((Collection<String>) element.getValue()).size();
      }
      ArrayList<VotingPage> voters = new ArrayList<VotingPage>();
      for (String voterName : element.getValue()) {
        if (!voterName.isEmpty()) {
          voters.add(new VotingPage(voterName, contributorVotes));
        }
      }
      receiver.output(KV.of(element.getKey(), new RankedPage(element.getKey(), voters)));
    }
  }
  
  static class Job2Mapper extends DoFn<KV<String, RankedPage>, KV<String, RankedPage>> {
	    @ProcessElement
	    public void processElement(@Element KV<String, RankedPage> element,
	        OutputReceiver<KV<String, RankedPage>> receiver) {
	      Integer votes = 0;
	      ArrayList<VotingPage> voters = element.getValue().getPagesVoted();
	      if (voters instanceof Collection) {
	        votes = ((Collection<VotingPage>) voters).size();
	      }

	      for (VotingPage vp : voters) {
	        String pageName = vp.getName();
	        Double pageRank = vp.getRank();
	        String contributingPageName = element.getKey();
	        Double contributingPageRank = element.getValue().getRankValue();
	        VotingPage contributor = new VotingPage(contributingPageName, contributingPageRank, votes);
	        ArrayList<VotingPage> arr = new ArrayList<>();
	        arr.add(contributor);
	        receiver.output(KV.of(vp.getName(), new RankedPage(pageName, pageRank, arr)));
	      }
	    }
	  }
  
  
  static class Job2Updater extends DoFn<KV<String, Iterable<RankedPage>>, KV<String, RankedPage>> {
	    @ProcessElement
	    public void processElement(@Element KV<String, Iterable<RankedPage>> element,
	        OutputReceiver<KV<String, RankedPage>> receiver) {
	      String thisPage = element.getKey();
	      Iterable<RankedPage> rankedPages = element.getValue();
	      Double dampingFactor = 0.85;
	      Double updatedRank = (1 - dampingFactor);
	      ArrayList<VotingPage> newVoters = new ArrayList<VotingPage>();

	      for (RankedPage pg : rankedPages) {
	        if (pg != null) {
	          for (VotingPage vp : pg.getPagesVoted()) {
	            newVoters.add(vp);
	            updatedRank += (dampingFactor) * vp.getRank() / (double) vp.getVotes();
	          }
	        }
	      }

	      receiver.output(KV.of(thisPage, new RankedPage(thisPage, updatedRank, newVoters)));
	    }
	  }
// I wrote this method with the help and Pramod and Sai Kiran Gangidi  
  static class Job3 extends DoFn<KV<String, RankedPage>, KV<String, Double>> {
	    @ProcessElement
	    public void processElement(@Element KV<String, RankedPage> element,
	        OutputReceiver<KV<String, Double>> receiver) {
	      String currentPage = element.getKey();
	      Double currentPageRank = element.getValue().getRankValue();

	      receiver.output(KV.of(currentPage, currentPageRank));
	    }
	  }
 // I wrote this method with the help and Pramod Gonegari and Sai Kiran Gangidi  
  public static class Job3Final implements Comparator<KV<String, Double>>, Serializable {
	    @Override
	    public int compare(KV<String, Double> value1, KV<String, Double> value2) {
	      return value1.getValue().compareTo(value2.getValue());
	    }
	  }
  


 // I wrote this method with the help and Pramod Gonegari and Sai Kiran Gangidi  
  public static void main(String[] args) {

    PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline p = Pipeline.create(options);
    String dataFolder = "web04";

    PCollection<KV<String, String>> pc1 = jarugulaMapper01(p,dataFolder, "go.md" );
    PCollection<KV<String, String>> pc2 = jarugulaMapper01(p, dataFolder,"python.md");
    PCollection<KV<String, String>> pc3 = jarugulaMapper01(p,dataFolder, "java.md");
    PCollection<KV<String, String>> pc4 = jarugulaMapper01(p, dataFolder,"README.md");

    PCollectionList<KV<String, String>> pCollectionList = PCollectionList.of(pc1).and(pc2).and(pc3).and(pc4);

    PCollection<KV<String, String>> mergedList = pCollectionList.apply(Flatten.<KV<String, String>>pCollections());
    PCollection<KV<String, Iterable<String>>> urlToDocs = mergedList.apply(GroupByKey.<String, String>create());

    PCollection<KV<String, RankedPage>> jobTwoin = urlToDocs.apply(ParDo.of(new Job1Finalizer()));

    PCollection<KV<String, RankedPage>> jobTwoout = null;

    PCollection<KV<String, RankedPage>> mappedKVs = null;

    PCollection<KV<String, Iterable<RankedPage>>> reducedKVs = null;
    int iterations = 50;
    for (int i = 1; i <= iterations; i++) {
      mappedKVs = jobTwoin.apply(ParDo.of(new Job2Mapper()));

      reducedKVs = mappedKVs
          .apply(GroupByKey.<String, RankedPage>create());

      jobTwoout = reducedKVs.apply(ParDo.of(new Job2Updater()));

      jobTwoin = jobTwoout;

    }
    PCollection<KV<String, Double>> maximumRank = jobTwoout.apply(ParDo.of(new Job3()));

    PCollection<KV<String, Double>> finalMaximum = maximumRank.apply(Combine.globally(Max.of(new Job3Final())));

    PCollection<String> fnl = finalMaximum.apply(MapElements.into(
        TypeDescriptors.strings())
        .via(kv -> kv.toString()));
    fnl.apply(TextIO.write().to("jarugulaOut"));

    p.run().waitUntilFinish();
    // PCollection<String> pLinksStr = urlToDocs.apply(
    //     MapElements.into(
    //         TypeDescriptors.strings())
    //         .via((mergeOut) -> mergeOut.toString()));

    // pLinksStr.apply(TextIO.write().to("jarugulaOut"));

    // p.run().waitUntilFinish();
  }

  private static PCollection<KV<String, String>> jarugulaMapper01(Pipeline p, String dataFolder, String dataFile) {
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
