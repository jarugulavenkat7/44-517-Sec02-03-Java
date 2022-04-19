package edu.nwmsu.sec02grp03.enugala;

import java.util.ArrayList;

public class RankedPageEnugala {
    String voter;
    ArrayList<VotingPageEnugala> voterList = new ArrayList<>();
    
    public RankedPageEnugala(String voter, ArrayList<VotingPageEnugala> voters){
        this.voter = voter;
        this.voterList = voters;
    }
}