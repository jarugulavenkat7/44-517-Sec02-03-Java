package edu.nwmissouri.sec02group03.arram;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public  class VotingPageArram extends DoFn<KV<String,Iterable<String>>,KV<String,RankedPageArram>> implements Serializable{
    String voterString;
    int contributedVotes;
    public VotingPageArram(String voterString,Integer contributedVotes){
        this.voterString = voterString;
        this.contributedVotes = contributedVotes;        
    }
    public String getvoterString() {
        return voterString;
    }
    public void setvoterString(String voterString) {
        this.voterString = voterString;
    }
    public int getcontributedVotes() {
        return contributedVotes;
    }
    public void setcontributedVotes(int contributedVotes) {
        this.contributedVotes = contributedVotes;
    }
    @Override
    public String toString() {
        return "contributedVotes=" + contributedVotes + ", voterString=" + voterString;
    }


}