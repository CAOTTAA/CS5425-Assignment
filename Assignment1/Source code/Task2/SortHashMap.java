package task2;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

public class SortHashMap {
	private List<Entry<String,Float>> list = new LinkedList<Entry<String,Float>>();
	/**
	 * inverted order
	 * @param map
	 * @return
	 */
	
	public static  List<Entry<String,Float>> sortHashMap(HashMap<String,Float> map){
		SortHashMap sorthashmap = new SortHashMap();
		
		sorthashmap.list.addAll(map.entrySet());
		
		Collections.sort(sorthashmap.list,new Comparator<Entry<String,Float>>(){
			public int compare(Entry obj1,Entry obj2){
				if(Float.parseFloat(obj1.getValue().toString())<Float.parseFloat(obj2.getValue().toString()))
					return 1;
				else if(Float.parseFloat(obj1.getValue().toString())==Float.parseFloat(obj2.getValue().toString()))
					return 0;
				else
					return -1;
			}
		});
		
		return sorthashmap.list;	
	}
	public static void main(String[]args){
		//for test
		HashMap<String, Float> omap = new HashMap<String, Float> ();
		omap.put("x", (float)(1.0));
		omap.put("y", (float)(3.0));
		omap.put("z", (float)(2.0));	
		List<Entry<String,Float>> list = new LinkedList<Entry<String,Float>>();
		list=SortHashMap.sortHashMap(omap);
		for(Entry<String,Float> ilist : list){
			System.out.println(ilist.getKey()+"\t"+ilist.getValue());
		}			
}
}

