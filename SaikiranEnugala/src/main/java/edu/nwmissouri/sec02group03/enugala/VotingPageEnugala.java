package edu.nwmissouri.sec02group03.enugala;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class VotingPageEnugala extends DoFn<KV<String,Iterable<String>>,KV<String,RankedPageEnugala>> implements Serializable{

		// TODO Auto-generated constructor stub
		String voterString;
        int contributedVotes;
        double pageRank = 1.0;
        public String getVoterString() {
            return voterString;
        }
        public void setVoterString(String voterString) {
            this.voterString = voterString;
        }
        public int getContributedVotes() {
            return contributedVotes;
        }
        public void setContributedVotes(int contributedVotes) {
            this.contributedVotes = contributedVotes;
        }
        public double getPageRank() {
            return pageRank;
        }
        public void setPageRank(double pageRank) {
            this.pageRank = pageRank;
        }
        public VotingPageEnugala(String voterString, int contributedVotes, double pageRank) {
            this.voterString = voterString;
            this.contributedVotes = contributedVotes;
            this.pageRank = pageRank;
        }
        
        public VotingPageEnugala(String voterString, int contributedVotes) {
            this.voterString = voterString;
            this.contributedVotes = contributedVotes;
        }
        @Override
        public String toString() {
            return "VotingPageEnugala [contributedVotes=" + contributedVotes + ", pageRank=" + pageRank
                    + ", voterString=" + voterString + "]";
        }

    

}
