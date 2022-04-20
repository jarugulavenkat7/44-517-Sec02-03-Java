package edu.nwmsu.sec02grp1.tata;

import java.io.Serializable;
import java.util.ArrayList;

public class RankedPageTata implements Serializable{
    String voter;
    ArrayList<VotingPageTata> voterList = new ArrayList<>();
    
    public RankedPageTata(String voter, ArrayList<VotingPageTata> voters){
        this.voter = voter;
        this.voterList = voters;
    }
    
    public String getVoter() {
        return voter;
    }

    public void setVoter(String voter) {
        this.voter = voter;
    }

    public ArrayList<VotingPageTata> getVoterList() {
        return voterList;
    }

    public void setVoterList(ArrayList<VotingPageTata> voterList) {
        this.voterList = voterList;
    }

    @Override
    public String toString(){
        return voter + voterList;
    }
}