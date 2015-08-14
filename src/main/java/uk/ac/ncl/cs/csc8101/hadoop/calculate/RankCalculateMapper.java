package uk.ac.ncl.cs.csc8101.hadoop.calculate;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class RankCalculateMapper extends Mapper<LongWritable, Text, Text, Text> {
	/**
     * The `map(...)` method is executed against each item in the input split. A key-value pair is
     * mapped to another, intermediate, key-value pair.
     *
     * Specifically, this method should take Text objects in the form
     *      `"[page]    [initialPagerank]    outLinkA,outLinkB,outLinkC..."`
     * and store a new key-value pair mapping linked pages to this page's name, rank and total number of links:
     *      `"[otherPage]   [thisPage]    [thisPagesRank]    [thisTotalNumberOfLinks]"
     *
     * Note: Remember that the pagerank calculation MapReduce job will run multiple times, as the pagerank will get
     * more accurate with each iteration. You should preserve each page's list of links.
     *
     * @param key the key associated with each item output from {@link uk.ac.ncl.cs.csc8101.hadoop.parse.PageLinksParseReducer PageLinksParseReducer}
     * @param value the text value "[page]  [initialPagerank]   outLinkA,outLinkB,outLinkC..."
     * @param context Mapper context object, to which key-value pairs are written
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
       //Split the Text objects in the form: [page]    [Pagerank]    outLinkA,outLinkB,outLinkC...  	
       String[] parse=value.toString().split("\t");
       
       //If value is invalid, then scrap it
       if(parse.length!=3)return;
       
        String source = parse[0]; //[sourcepage]
        String pageRank = parse[1];//[pagerank]
        String targetPage = parse[2];//[outlinks]

        //If value is invalid, then scrap it
        if(source.isEmpty()||pageRank.isEmpty()||targetPage.isEmpty()){return;}
        
        //Split all the outlinks and generate an array of it
        String[] targets=targetPage.split(",");  
        
        
        for(String page: targets)
        {
        	String content=source+"\t"+pageRank+"\t"+targets.length;
        	//add valid message to the map in the form [target], [source] [pagerank] [number of outlinks]
        	context.write(new Text(page), new Text(content));
        	
        }
        String targetstr="!\t"+targetPage;      
        
        //add the the outlinks message of a source page in the form [source], ! [outlink0],[outlink1]...
        context.write(new Text(source), new Text(targetstr));
        
    }
    
    
    


   

  
   
    }

