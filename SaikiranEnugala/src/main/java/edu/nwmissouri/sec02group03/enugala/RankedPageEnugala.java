package edu.nwmissouri.sec02group03.enugala;

import java.util.ArrayList;

public class RankedPageEnugala {
String key;
ArrayList<VotingPageEnugala> voters;
    public RankedPageEnugala(String voter, ArrayList<VotingPageEnugala> voters){
        this.key = voter;
        this.voters = voters;
    }
    @Override
    public String toString(){
        return key + voters;
    }
	

}
