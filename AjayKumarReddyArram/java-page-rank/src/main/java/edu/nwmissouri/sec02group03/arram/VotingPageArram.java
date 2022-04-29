package edu.nwmsu.sec02grp1.Arram;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.transforms.DoFn;


public  class VotingPageArram extends DoFn<KV<String,Iterable<String>>,KV<String,RankedPageArram>> implements Serializable{

    String voterName;
    int contributorVotes;
    double pageRank = 1.0;

    /**
     * 
     * @param voterName
     * @param votesContributed
     * @param pageRank
     */
    public VotingPageArram(String voterName,Integer votesContributed, double pageRank){
        this.voterName = voterName;
        this.contributorVotes = votesContributed;      
        this.pageRank = pageRank;  
    }

    /**
     * 
     * @param voterName
     * @param votesContributed
     */
    public VotingPageArram(String voterName,Integer votesContributed){
        this.voterName = voterName;
        this.contributorVotes = votesContributed;      
   
    }
    
    /**
     * 
     * @return 
     */
    public String getVoterName() {
        return voterName;
    }

    /**
     * 
     * @param voterName
     */
    public void setVoterName(String voterName) {
        this.voterName = voterName;
    }

    /**
     * 
     * @return 
     */
    public int getContributorVotes() {
        return contributorVotes;
    }

    /**
     * 
     * @param contributorVotes
     */
    public void setContributorVotes(int contributorVotes) {
        this.contributorVotes = contributorVotes;
    }

    /**
     * return 
     */
    @Override
    public String toString() {
        return "voterName = "+ voterName +", Page rank = "+this.pageRank +" ContributorVotes = " + contributorVotes;
    }

    /**
     * 
     * @return 
     */
    public double getPageRank() {
        return this.pageRank;
    }

    /**
     * 
     * @param pageRank
     */
    public void setPageRank(double pageRank){
        this.pageRank = pageRank;
    }


}