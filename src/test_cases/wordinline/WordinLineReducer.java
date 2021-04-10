package test_cases.wordinline;
import mapreduce.utils.Reducer;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;

public class WordinLineReducer extends Reducer{

    public List<String> reduce(String word,List<String> lines)

    {
         List<String> result = new ArrayList<String>();

         StringBuilder sb = new StringBuilder();
         for(String line : lines)
         {
             sb.append(line);
             sb.append(",");
         }

         if(sb.length() > 0)
         {
             sb.deleteCharAt(sb.length()-1);
         }

         String res = sb.toString();

         result.add(res);

         return result;


    }
}
