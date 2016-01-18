import java.io.*;
import oracle.stellent.ridc.*;
import oracle.stellent.ridc.model.*;
import oracle.stellent.ridc.protocol.*;
import oracle.stellent.ridc.protocol.intradoc.*;
import oracle.stellent.ridc.common.log.*;
import oracle.stellent.ridc.model.serialize.*;

/*
 * @author Sterin- Oracle Inc
 * 
 * This is a class used to test the basic functionality
 * of submitting a search to Content Server using RIDC.  
 * The response is then used to retrieve metadata about
 * the content items.  
 */

public class  TestRIDCGetContents {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		IdcClientManager manager = new IdcClientManager ();
		try{
			// Create a new IdcClient Connection using idc protocol (i.e. socket connection to Content Server)
			IdcClient idcClient = manager.createClient (args[0]);

                       IdcContext userContext = new IdcContext ("weblogic","welcome1");

			// Create an HdaBinderSerializer; this is not necessary, but it allows us to serialize the request and response data binders
			HdaBinderSerializer serializer = new HdaBinderSerializer ("UTF-8", idcClient.getDataFactory ());
			
			// Create a new binder for submitting a search
			DataBinder dataBinder = idcClient.createBinder();
       
            dataBinder.putLocal("IdcService", "GET_BATCH_FILE_DOCUMENTS");
            dataBinder.putLocal("IDC_Name","ecmtest2idcoraclecom16455");
            dataBinder.putLocal("aArchiveName","SourceInd");
            dataBinder.putLocal("aBatchFile",args[1]);




            // Write the data binder for the request to stdout
            serializer.serializeBinder (System.out, dataBinder);
           
            // Send the request to Content Server
            ServiceResponse response = idcClient.sendRequest(userContext,dataBinder);
            
            // Get the data binder for the response from Content Server
            DataBinder responseData = response.getResponseAsBinder();
            // Write the response data binder to stdout
         //   serializer.serializeBinder (System.out, responseData);
            // Retrieve the SearchResults ResultSet from the response
            DataResultSet resultSet = responseData.getResultSet("ExportResults");
           
               
 
            // Iterate over the ResultSet, retrieve properties from the content items
            for (DataObject dataObject : resultSet.getRows ()) {
             //   System.out.println ("ContentID is: " + dataObject.get ("aBatchFile"));
               System.out.println ("ContentID is : " + dataObject.get ("dDocName"));
               System.out.println ("Revision of item archived is: " + dataObject.get ("dRevisionID"));    
          //      System.out.println ("folder is  : " + dataObject.get ("xCollectionID"));
            //     System.out.println ("######");
            }
            
System.out.println("===TotalRows:" +responseData.getLocal("TotalRows"));
			
		} catch (IdcClientException ice){
			ice.printStackTrace();
		} catch (IOException ioe){
			ioe.printStackTrace();
		}
	}

}
