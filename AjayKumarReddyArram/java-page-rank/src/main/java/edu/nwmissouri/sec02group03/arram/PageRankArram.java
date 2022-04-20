/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements. See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership. The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package edu.nwmissouri.sec02group03.arram;



import java.util.ArrayList;

// beam-playground:
// name: MinimalWordCount
// description: An example that counts words in Shakespeare's works.
// multifile: false
// pipeline_options:
// categories:
// - Combiners
// - Filtering
// - IO
// - Core Transforms



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
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

public class PageRankArram {


  static class Job1Finalizer extends DoFn<KV<String, Iterable<String>>, KV<String, RankedPageArram>> {
    @ProcessElement
    public void processElement(@Element KV<String, Iterable<String>> element,
        OutputReceiver<KV<String, RankedPageArram>> receiver) {
      Integer contributorVotes = 0;
      if (element.getValue() instanceof Collection) {
        contributorVotes = ((Collection) element.getValue()).size();
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

  public static void main(String[] args) {

    PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline p = Pipeline.create(options);
    String dataFolder = "web04";
    PCollection<KV<String, String>> pcol1 = ArramMapper(p, dataFolder, "go.md");
    PCollection<KV<String, String>> pcol2 = ArramMapper(p, dataFolder, "python.md");
    PCollection<KV<String, String>> pcol3 = ArramMapper(p, dataFolder, "java.md");
    PCollection<KV<String, String>> pcol4 = ArramMapper(p, dataFolder, "README.md");

    PCollectionList<KV<String, String>> pcolList = PCollectionList.of(pcol1).and(pcol2).and(pcol3).and(pcol4);

    PCollection<KV<String, String>> mergedList = pcolList.apply(Flatten.<KV<String, String>>pCollections());

    PCollection<KV<String, Iterable<String>>> pColGroupBy = mergedList.apply(GroupByKey.create());
   
    PCollection<KV<String, RankedPageArram>> job2in = pColGroupBy.apply(ParDo.of(new Job1Finalizer()));

   
    PCollection<String> pLinksStr = job2in.apply(
        MapElements.into(
            TypeDescriptors.strings())
            .via((mergeOut) -> mergeOut.toString()));

    pLinksStr.apply(TextIO.write().to("Arramout"));

    p.run().waitUntilFinish();
  }

  private static PCollection<KV<String, String>> ArramMapper(Pipeline p, String dataFolder, String dataFile) {
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