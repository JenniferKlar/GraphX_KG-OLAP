package Converter;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Scanner;

import org.apache.jena.query.Dataset;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;

public class Main {

	public static void main(String[] args) {

		readWrite("C:\\Users\\jenniffer\\Desktop\\Masterarbeit\\bd108186-5adb-4f00-b5fe-b24a8993560b");

	}
	public static void readWrite(String fileName) {
		/*Model model = ModelFactory.createDefaultModel() ;
		model.read("C:\\Users\\jenniffer\\Dropbox\\Masterarbeit\\bd108186-5adb-4f00-b5fe-b24a8993560b.trig");
		
		FileWriter out;
		try {
			out = new FileWriter("C:\\Users\\jenniffer\\Dropbox\\Masterarbeit\\bd108186-5adb-4f00-b5fe-b24a8993560b.nq");
			RDFDataMgr.write(out, model, Lang.NQUADS);
			System.out.println(model.size());
		} catch (IOException e) {
			e.printStackTrace();
		}*/
		Dataset dataset = RDFDataMgr.loadDataset(fileName + ".trig") ;
		//FileWriter out;
		//out = new FileWriter("C:\\Users\\jenniffer\\Dropbox\\Masterarbeit\\bd108186-5adb-4f00-b5fe-b24a8993560b.nq");
		//RDFDataMgr.write(out, dataset, Lang.NQUADS);
		//RDFDataMgr.write(System.out, dataset, Lang.NQUADS);
		
	    try(OutputStream out = new FileOutputStream(fileName + ".nq")) {
	        RDFDataMgr.write(out, dataset, Lang.NQUADS);
	    } catch (FileNotFoundException e) {
	        e.printStackTrace();
	    } catch (IOException e) {
	        e.printStackTrace();
	    }

	}

}
