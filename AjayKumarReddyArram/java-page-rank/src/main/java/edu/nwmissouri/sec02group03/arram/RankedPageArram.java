package edu.nwmsu.sec02grp1.Arram;

import java.io.Serializable;
import java.util.ArrayList;

import org.apache.beam.sdk.values.KV;

public class RankedPageArram implements Serializable{
    String voter;
    double pageRank = 1.0;
    ArrayList<VotingPageArram> voterList = new ArrayList<>();
    
    /**
     * 
     * @param voter
     * @param pageRank
     * @param voters
     */
    public RankedPageArram(String voter,double pageRank, ArrayList<VotingPageArram> voters){
        this.voter = voter;
        this.voterList = voters;
        this.pageRank = pageRank;
    }    

    public RankedPageArram() {
        voter = "";
        pageRank = 0.0;
    }

    /**
     * 
     * @param voter
     * @param voters
     */
    public RankedPageArram(String voter, ArrayList<VotingPageArram> voters){
        this.voter = voter;
        this.voterList = voters;
    }    
    
    /**
     * 
     * @return 
     */
    public String getVoter() {
        return voter;
    }

    /**
     * 
     * @param 
     */
    public void setVoter(String voter) {
        this.voter = voter;
    }
    public double getPageRank() {
        return this.pageRank;
    }
    /**
     * 
     * @return 
     */
    public ArrayList<VotingPageArram> getVoterList() {
        return voterList;
    }

    /**
     * 
     * @param voterList
     */
    public void setVoterList(ArrayList<VotingPageArram> voterList) {
        this.voterList = voterList;
    }

    /**
     * @return
     */
    @Override
    public String toString(){
        return this.voter +"<"+ this.pageRank +","+ voterList +">";
    }
    

}