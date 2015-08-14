package uk.ac.ncl.cs.csc8101.hadoop.calculate;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;


public class RankCalculateReducer extends Reducer<Text, Text, Text, Text> {
	private static final double d = 0.85;
    /**
     * The `reduce(...)` method is called for each <key, (Iterable of values)> pair in the grouped input.
     * Output values must be of the same type as input values and Input keys must not be altered.
     *
     * Specifically, this method should take the iterable of links to a page, along with their pagerank and number of links.
     * It should then use these to increase this page's rank by its share of the linking page's:
     *      thisPagerank +=  linkingPagerank> / count(linkingPageLinks)
     *
     * Note: remember pagerank's dampening factor.
     *
     * Note: Remember that the pagerank calculation MapReduce job will run multiple times, as the pagerank will get
     * more accurate with each iteration. You should preserve each page's list of links.
     *
     * @param page The individual page whose rank we are trying to capture
     * @param values The Iterable of other pages which link to this page, along with their pagerank and number of links
     * @param context The Reducer context object, to which key-value pairs are written
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void reduce(Text page, Iterable<Text> values, Context context) throws IOException, InterruptedException
    {
    	double sum=0;
    	String target="";
    	
    	
    	for(Text value: values)
    	{
    		String str = value.toString();
    		String[] data = str.split("\t");
    		
    		//record the outlink pages
    		if(data.length==2&&data[0].equals("!"))
    		{target=data[1];}		
    		
    		else if(data.length==3)
    		{
    			// calculate the sum of (PR(T1)/C(T1) + ... + PR(Tn)/C(Tn))
    			sum+=Double.parseDouble(data[1])/Integer.parseInt(data[2]);
    			
    		}
    		
    	}
    	
    	//calculate the value PR(A) according to the sum
    	sum=1-d+d*sum;
    	
    	//write the page information in the form: [source], [pagerank] [outlinks]. [outlinks] could be "".
    	context.write(page,new Text(String.format("%.5f", sum)+"\t"+target));
    	
    	
    }
    
}
