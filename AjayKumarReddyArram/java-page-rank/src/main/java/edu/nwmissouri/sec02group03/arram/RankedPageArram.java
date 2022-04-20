package edu.nwmissouri.sec02group03.arram;

import java.io.Serializable;
import java.util.ArrayList;

public class RankedPageArram implements Serializable{
    String voter;
    ArrayList<VotingPageArram> voterList = new ArrayList<>();
    
    public RankedPageArram(String voter, ArrayList<VotingPageArram> voters){
        this.voter = voter;
        this.voterList = voters;
    }
    @Override
    public String toString(){
        return voter + voterList;
    }
}
