package edu.nwmsu.sec02grp1.reddy;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public  class VotingPageEnugala extends DoFn<KV<String,Iterable<String>>,KV<String,RankedPageReddy>>{
    String voterName;
    int contributorVotes;
    public VotingPageEnugala(String voterName,Integer contributorVotes2){
        this.voterName = voterName;
        this.contributorVotes = contributorVotes2;        
    }
}