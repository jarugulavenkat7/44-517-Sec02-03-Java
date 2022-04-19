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
package edu.nwmsu.sec02grp03.enugala;



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
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

public class MinimalPageRankEnugala {

  public static void main(String[] args) {

    PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline p = Pipeline.create(options);
    String dataFolder = "web04";
    String dataFile = "go.md";
    PCollection<KV<String, String>> pcol1 = EnugalaMapper(p, dataFolder, "go.md");
    PCollection<KV<String, String>> pcol2 = EnugalaMapper(p, dataFolder, "python.md");
    PCollection<KV<String, String>> pcol3 = EnugalaMapper(p, dataFolder, "java.md");
    PCollection<KV<String, String>> pcol4 = EnugalaMapper(p, dataFolder, "README.md");

    PCollectionList<KV<String, String>> pcolList = PCollectionList.of(pcol1).and(pcol2).and(pcol3).and(pcol4);

    PCollection<KV<String, String>> mergedList = pcolList.apply(Flatten.<KV<String, String>>pCollections());
    PCollection<String> pLinksStr = mergedList.apply(
        MapElements.into(
            TypeDescriptors.strings())
            .via((mergeOut) -> mergeOut.toString()));

    pLinksStr.apply(TextIO.write().to("Enugala_out"));

    p.run().waitUntilFinish();
  }

  private static PCollection<KV<String, String>> EnugalaMapper(Pipeline p, String dataFolder, String dataFile) {
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