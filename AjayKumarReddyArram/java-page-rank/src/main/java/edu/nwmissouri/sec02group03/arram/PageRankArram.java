
package edu.nwmissouri.sec02group03.arram;

import java.util.ArrayList;
import java.io.Serializable;



// beam-playground:
//   name: MinimalWordCount
//   description: Word count in Shakespeares poem
//   categories:     Combiners,Filtering,IO,Core Transforms


import java.util.Collection;
import java.util.Arrays;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.Pipeline;
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
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

import io.grpc.netty.shaded.io.netty.handler.pcap.PcapWriteHandler;

/**
 * An example that counts words in Shakespeare.
 *
 * <p>
 * This class, {@link MinimalWordCount}, is the first in a series of four
 * successively more
 * detailed 'word count' examples. Here, for simplicity, we don't show any
 * error-checking or
 * argument processing, and focus on construction of the pipeline, which chains
 * together the
 * application of core transforms.
 *
 * <p>
 * Next, see the {@link WordCount} pipeline, then the
 * {@link DebuggingWordCount}, and finally the
 * {@link WindowedWordCount} pipeline, for more detailed examples that introduce
 * additional
 * concepts.
 *
 * <p>
 * Concepts:
 *
 * <pre>
 *   1. Reading data from text files
 *   2. Specifying 'inline' transforms
 *   3. Counting items in a PCollection
 *   4. Writing data to text files
 * </pre>
 *
 * <p>
 * No arguments are required to run this pipeline. It will be executed with the
 * DirectRunner. You
 * can see the results in the output files in your current working directory,
 * with names like
 * "wordcounts-00001-of-00005. When running on a distributed service, you would
 * use an appropriate
 * file service.
 */

public class MinimalPageRankArram {
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
  static class Job1Finalizer extends DoFn<KV<String, Iterable<String>>, KV<String, RankedPageArram>> {
    @ProcessElement
    public void processElement(@Element KV<String, Iterable<String>> element,
        OutputReceiver<KV<String, RankedPageArram>> receiver) {
      Integer contributorVotes = 0;
      if (element.getValue() instanceof Collection) {
        contributorVotes = ((Collection<String>) element.getValue()).size();
      }
      ArrayList<VotingPageArram> voters = new ArrayList<VotingPageArram>();
      for (String voterName : element.getValue()) {
        if (!voterName.isEmpty()) {
          voters.add(new VotingPageArram(voterName, contributorVotes));
        }
      }
      receiver.output(KV.of(element.getKey(), new RankedPageArram(element.getKey(), voters)));
    }
  }

  static class Job2Mapper extends DoFn<KV<String, RankedPageArram>, KV<String, RankedPageArram>> {
    @ProcessElement
    public void processElement(@Element KV<String, RankedPageArram> element,
        OutputReceiver<KV<String, RankedPageArram>> receiver) {
      int votes = 0;
      ArrayList<VotingPageArram> voters = element.getValue().getVoterList();
      if (voters instanceof Collection) {
        votes = ((Collection<VotingPageArram>) voters).size();
      }
      for (VotingPageArram vp : voters) {
        String pageName = vp.getVoterName();
        double pageRank = vp.getPageRank();
        String contributingPageName = element.getKey();
        double contributingPageRank = element.getValue().getRank();
        VotingPageArram contributor = new VotingPageArram(contributingPageName, votes, contributingPageRank);
        ArrayList<VotingPageArram> arr = new ArrayList<>();
        arr.add(contributor);
        receiver.output(KV.of(vp.getVoterName(), new RankedPageArram(pageName, pageRank, arr)));
      }
    }
  }

  static class Job2Updater extends DoFn<KV<String, Iterable<RankedPageArram>>, KV<String, RankedPageArram>> {
    @ProcessElement
    public void processElement(@Element KV<String, Iterable<RankedPageArram>> element,
        OutputReceiver<KV<String, RankedPageArram>> receiver) {
      Double dampingFactor = 0.85;
      Double updatedRank = (1 - dampingFactor);
      ArrayList<VotingPageArram> newVoters = new ArrayList<>();
      for (RankedPageArram rankPage : element.getValue()) {
        if (rankPage != null) {
          for (VotingPageArram votingPage : rankPage.getVoterList()) {
            newVoters.add(votingPage);
            updatedRank += (dampingFactor) * votingPage.getPageRank() / (double) votingPage.getContributorVotes();
          }
        }
      }
      receiver.output(KV.of(element.getKey(), new RankedPageArram(element.getKey(), updatedRank, newVoters)));

    }

  }


  public static void main(String[] args) {

    PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline p = Pipeline.create(options);

    final String folderName = "web04";
    
    PCollection<KV<String, String>> pColLinksGo = ArramPcolLinks(p, folderName, "go.md");
    PCollection<KV<String, String>> pColLinksJava = ArramPcolLinks(p, folderName, "java.md");
    PCollection<KV<String, String>> pColLinksPython = ArramPcolLinks(p, folderName, "python.md");
    PCollection<KV<String, String>> pColLinksReadme = ArramPcolLinks(p, folderName, "readme.md");
    // Adding PCollection to PCollection list
    PCollectionList<KV<String, String>> pColList = PCollectionList.of(pColLinksGo).and(pColLinksJava)
        .and(pColLinksPython).and(pColLinksReadme);
    // Merging into single list
    PCollection<KV<String, String>> pColListMerged = pColList.apply(Flatten.<KV<String, String>>pCollections());
    PCollection<KV<String, Iterable<String>>> pColGroupByKey = pColListMerged.apply(GroupByKey.create());
    //job2
    PCollection<KV<String, RankedPageArram>> job2in = pColGroupByKey.apply(ParDo.of(new Job1Finalizer()));

    PCollection<KV<String, RankedPageArram>> job2out = null;
    int iterations = 40;
    for (int i = 1; i <= iterations; i++) {
      // use job2in to calculate job2 out
      PCollection<KV<String, RankedPageArram>> job2Mapper = job2in.apply(ParDo.of(new Job2Mapper()));

      PCollection<KV<String, Iterable<RankedPageArram>>> job2MapperGrpByKey = job2Mapper.apply(GroupByKey.create());

      job2out = job2MapperGrpByKey.apply(ParDo.of(new Job2Updater()));
      job2in = job2out;
    }

    
    PCollection<String> pColStringLists = job2out.apply(
        MapElements.into(
            TypeDescriptors.strings()).via(
                kvtoString -> kvtoString.toString()));
    // Write the output to the file

    pColStringLists.apply(TextIO.write().to("PageRank-Arram"));

    p.run().waitUntilFinish();
  }

  private static PCollection<KV<String, String>> ArramPcolLinks(Pipeline p, final String folderName,
      final String fileName) {
    // Fetching the data from the destination
    PCollection<String> pColInputLines = p.apply(TextIO.read().from(folderName + "/" + fileName));
    // taking the lines only which starts with [
    PCollection<String> pColLinkLines = pColInputLines.apply(Filter.by((String line) -> line.startsWith("[")));

    // Take the outgoing links from the format []("")
    PCollection<String> pColLinks = pColLinkLines.apply(
        MapElements.into(
            TypeDescriptors.strings())
            .via(
                (String linkLine) -> linkLine.substring(linkLine.indexOf("(") + 1, linkLine.indexOf(")"))));

    // Map the links with the file name passed to it
    PCollection<KV<String, String>> pColkvs = pColLinks.apply(
        MapElements.into(
            TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings())).via(
                outgoingLink -> KV.of(fileName, outgoingLink)));
    // Return the KV pairs
    return pColkvs;
  }
}