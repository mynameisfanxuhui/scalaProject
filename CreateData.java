import java.io.*;

// this is the create data  for Problem2
// @author: xli14@wpi.edu


public class CreateData {

    private int GenerateRandom(int min, int max){
        int random = 0;
        random = (int) Math.round(Math.random() * (max - min)) + min;
        return random;
    }

    // Create data for Dataset P
    // Returns "x,y" in String
    private void CreateP(){
        File file = new File("DatasetP.txt");
        try{
            FileWriter fw = new FileWriter(file);
            BufferedWriter bw = new BufferedWriter(fw);
            for (int i = 0; i<10000000; i++){
                String line = String.valueOf(GenerateRandom(1,10000))+","+String.valueOf(GenerateRandom(1,10000));
                bw.write(line);
                bw.newLine();
            }
            bw.close();
            fw.close();
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    // main function
    public static void main(String[] args){
        CreateData create = new CreateData();
        create.CreateP();
    }
}
