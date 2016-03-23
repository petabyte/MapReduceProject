import org.apache.hadoop.io.Text;

/**
 * Created by George on 3/21/2016.
 */
public class KeywordParser{
    private String keyWord = null;

    public void parseKeyWord(Text lineRecordText){
        keyWord = null;
        String lineRecord = lineRecordText.toString();
        String [] lineSplit = lineRecord.split("\\t+");
        String firstElement = lineSplit[1];
        if( firstElement != null && firstElement.length() > 0 ){
            //Replace all multiple white space
            keyWord = firstElement.replaceAll("\\s+"," ");
            //Replace double - double quotes
            keyWord = keyWord.replaceAll("^\"\"", "\"");
            //Replace double -  double quotes at the end
            keyWord = keyWord.replaceAll("\"\"$","\"");
            //Remove space after quote if keyword has quote
            keyWord = keyWord.replaceAll("^\"\\s", "\"");
            //Remove space before closing quote
            keyWord = keyWord.replaceAll("\\s\"$","\"");
            //Make it lower case
            keyWord = keyWord.toLowerCase();
        }
    }

    public boolean foundKeyWord(){
        if(keyWord != null && keyWord.length() > 0){
            return true;
        }
        return false;
    }

    public String getKeyWord(){
        return keyWord;
    }
}
