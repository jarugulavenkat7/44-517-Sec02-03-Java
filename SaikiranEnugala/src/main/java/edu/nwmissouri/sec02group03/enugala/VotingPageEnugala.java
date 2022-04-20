package edu.nwmissouri.sec02group03.enugala;

public class VotingPageEnugala {

		// TODO Auto-generated constructor stub
		String voterString;
        int contributedVotes;

    public VotingPageEnugala(String voterString,Integer contributedVotes){
        this.voterString = voterString;
        this.contributedVotes = contributedVotes;        
    }
    @Override
    public String toString() {
        return "contributedVotes=" + contributedVotes + ", voterString=" + voterString;
    }
	

}
