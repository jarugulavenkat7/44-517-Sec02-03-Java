package edu.nwmissouri.sec02group03.enugala;

import java.util.ArrayList;

public class RankedPageEnugala {
    String voter;
    double rank;
    ArrayList<VotingPageEnugala> voterList = new ArrayList<>();

    public RankedPageEnugala(String voter, double rank, ArrayList<VotingPageEnugala> voters) {
        this.voter = voter;
        this.voterList = voters;
        this.rank = rank;

    }

    public RankedPageEnugala(String voter, ArrayList<VotingPageEnugala> voterList) {
        this.voter = voter;
        this.voterList = voterList;
    }

    public String getVoter() {
        return voter;
    }

    public void setVoter(String voter) {
        this.voter = voter;
    }

    public double getRank() {
        return rank;
    }

    public void setRank(double rank) {
        this.rank = rank;
    }

    public ArrayList<VotingPageEnugala> getVoterList() {
        return voterList;
    }

    public void setVoterList(ArrayList<VotingPageEnugala> voterList) {
        this.voterList = voterList;
    }
}