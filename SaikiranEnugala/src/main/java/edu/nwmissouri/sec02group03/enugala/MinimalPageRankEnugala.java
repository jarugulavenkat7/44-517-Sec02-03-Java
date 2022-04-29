package edu.nwmissouri.sec02group03.enugala;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
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

public class MinimalPageRankEnugala {


   // DEFINE DOFNS
  // ==================================================================
  // You can make your pipeline assembly code less verbose by defining
  // your DoFns statically out-of-line.
  // Each DoFn<InputT, OutputT> takes previous output
  // as input of type InputT
  // and transforms it to OutputT.
  // We pass this DoFn to a ParDo in our pipeline.

  /**
   * DoFn Job1Finalizer takes KV(String, String List of outlinks) and transforms
   * the value into our custom RankedPage Value holding the page's rank and list
   * of voters.
   * 
   * The output of the Job1 Finalizer creates the initial input into our
   * iterative Job 2.
   */
  static class Job1Finalizer extends DoFn<KV<String, Iterable<String>>, KV<String, RankedPageEnugala>> {
    
    @ProcessElement
    public void processElement(@Element KV<String, Iterable<String>> element,
        OutputReceiver<KV<String, RankedPageEnugala>> receiver) {
      Integer contributorVotes = 0;
      if (element.getValue() instanceof Collection) {
        contributorVotes = ((Collection<String>) element.getValue()).size();
      }
      ArrayList<VotingPageEnugala> voters = new ArrayList<VotingPageEnugala>();
      for (String voterName : element.getValue()) {
        if (!voterName.isEmpty()) {
          voters.add(new VotingPageEnugala(voterName, contributorVotes));
        }
      }
      receiver.output(KV.of(element.getKey(), new RankedPageEnugala(element.getKey(), voters)));
    }
  }
   static class Job2Mapper extends DoFn<KV<String, RankedPage>, KV<String, RankedPage>> {
    @ProcessElement
    public void processElement(@Element KV<String, RankedPage> element,
      OutputReceiver<KV<String, RankedPage>> receiver) {
      int votes = 0;
      ArrayList<VotingPage> voters = element.getValue().getVoterList();
      if(voters instanceof Collection){
        votes = ((Collection<VotingPage>) voters).size();
      }
      for(VotingPage vp: voters){
        String pageName = vp.getVoterName();
        double pageRank = vp.getPageRank();
        String contributingPageName = element.getKey();
        double contributingPageRank = element.getValue().getRank();
        VotingPage contributor = new VotingPage(contributingPageName,votes,contributingPageRank);
        ArrayList<VotingPage> arr = new ArrayList<>();
        arr.add(contributor);
        receiver.output(KV.of(vp.getVoterName(), new RankedPage(pageName, pageRank, arr)));        
      }
    }
  }

  static class Job2Updater extends DoFn<KV<String, Iterable<RankedPage>>, KV<String, RankedPage>> {
    @ProcessElement
    public void processElement(@Element KV<String, Iterable<RankedPage>> element,
      OutputReceiver<KV<String, RankedPage>> receiver) {
        Double dampingFactor = 0.85;
        Double updatedRank = (1 - dampingFactor);
        ArrayList<VotingPage> newVoters = new ArrayList<>();
        for(RankedPage rankPage:element.getValue()){
          if (rankPage != null) {
            for(VotingPage votingPage:rankPage.getVoterList()){
              newVoters.add(votingPage);
              updatedRank += (dampingFactor) * votingPage.getPageRank() / (double)votingPage.getContributorVotes();
            }
          }
        }
        receiver.output(KV.of(element.getKey(),new RankedPage(element.getKey(), updatedRank, newVoters)));

    }

  }

  public static void main(String[] args) {

    PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline p = Pipeline.create(options);
    String dataFolder = "web04";

    PCollection<KV<String, String>> pc1 = enugalaMapper01(p,dataFolder, "go.md" );
    PCollection<KV<String, String>> pc2 = enugalaMapper01(p, dataFolder,"python.md");
    PCollection<KV<String, String>> pc3 = enugalaMapper01(p,dataFolder, "java.md");
    PCollection<KV<String, String>> pc4 = enugalaMapper01(p, dataFolder,"README.md");

    PCollectionList<KV<String, String>> pCollectionList = PCollectionList.of(pc1).and(pc2).and(pc3).and(pc4);

    PCollection<KV<String, String>> mergedList = pCollectionList.apply(Flatten.<KV<String, String>>pCollections());
    PCollection<KV<String, Iterable<String>>> pCollectionGroupByKey = myMergedList.apply(GroupByKey.create());

    PCollection<KV<String, RankedPage>> job02Input = pCollectionGroupByKey.apply(ParDo.of(new Job1Finalizer()));

    PCollection<KV<String,RankedPage>> job2Mapper = job02Input.apply(ParDo.of(new Job2Mapper()));

    PCollection<KV<String, RankedPage>> job02Output = null;
    PCollection<KV<String,Iterable<RankedPage>>> job02MapperGroupbkey = job2Mapper.apply(GroupByKey.create());

    job02Output = job02MapperGroupbkey.apply(ParDo.of(new Job2Updater()));

    job02MapperGroupbkey = job02Output.apply(GroupByKey.create());


    PCollection<String> pLinksStr = job02Output.apply(
        MapElements.into(
            TypeDescriptors.strings())
            .via((mergeOut) -> mergeOut.toString()));

    pLinksStr.apply(TextIO.write().to("Enugala_Out"));

    p.run().waitUntilFinish();
  }

  private static PCollection<KV<String, String>> enugalaMapper01(Pipeline p, String dataFolder, String dataFile) {
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
