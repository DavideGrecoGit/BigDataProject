package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;
import java.util.List;

import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;

public class RankedResultsList implements Serializable{
    List<RankedResult> list;

    public RankedResultsList(List<RankedResult> resultsList) {
        this.list = resultsList;
    }
    public List<RankedResult> getList(){
        return this.list;
    }
    public void setList(List<RankedResult> resultsList){
        this.list = resultsList;
    }

}
