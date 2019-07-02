import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class ConfigFile {

    String filepath;

    public ConfigFile(String filepath) throws IOException {
        this.filepath = filepath;
    }

    public String getFilepath() {
        return filepath;

    }


    public void setFilepath(String filepath) {
        this.filepath = filepath;
    }

    public String getFileName(String s) {
        String filename="";
        try {
            FileReader fileReader = new FileReader(this.filepath);
            BufferedReader br = new BufferedReader(fileReader);
            String line="";
            while ( (line = br.readLine() ) != null) {
                System.out.println(line);
                if ( line.contains(s)) {
                    String[] arr= line.split("=");
                    System.out.println(arr[0] + "   " + arr[1]);
                    filename = arr[1];
                }
            }
        }
        catch (Exception e){
            System.out.println("Exception: " + e);
        }
        if (filename != "")
            return filename;
        else
            return "Error";

    }



}
