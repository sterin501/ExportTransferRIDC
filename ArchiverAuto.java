import java.io.*;
import oracle.stellent.ridc.*;
import oracle.stellent.ridc.model.*;
import oracle.stellent.ridc.protocol.*;
import oracle.stellent.ridc.protocol.intradoc.*;
import oracle.stellent.ridc.common.log.*;
import oracle.stellent.ridc.model.serialize.*;
import java.util.*;

/*
 * @author Sterin- Oracle Inc
 * 
 * This is a class used to test the basic functionality
 * of submitting a search to Content Server using RIDC.  
 * The response is then used to retrieve metadata about
 * the content items.  
 */

public class ArchiverAuto {

static List<String> aBatchFile  = new ArrayList<String>(); 
static List<String> aContenID  = new ArrayList<String>(); 

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		IdcClientManager manager = new IdcClientManager ();

                  	Properties prop = new Properties();
	                InputStream input = null;
                    
               

                    
		try{
			

               input = new FileInputStream("config.properties");
 
		// load a properties file
		prop.load(input);


       // Create a new IdcClient Connection using idc protocol (i.e. socket connection to Content Server)
			IdcClient idcClient = manager.createClient (prop.getProperty("url"));

                       IdcContext userContext = new IdcContext (prop.getProperty("user"),prop.getProperty("password"));

			// Create an HdaBinderSerializer; this is not necessary, but it allows us to serialize the request and response data binders
			HdaBinderSerializer serializer = new HdaBinderSerializer ("UTF-8", idcClient.getDataFactory ());
			
			// Create a new binder
			DataBinder dataBinder = idcClient.createBinder();

                        System.out.println("Step 1 : Exporting Contents   .....");



                           runExport  (prop,idcClient,userContext,dataBinder);

                                   System.out.println("Sleeping for 5 secs   .....");

                                              Thread.sleep(5000);                 //Sleeping for 5 secs 
                                     

                            System.out.println("Step 2: Getting Batch Files of Archiver Collection  .....");

                           
                        // To get batch file 
                        runGetBatchFile (prop,idcClient,userContext,dataBinder);


                                     System.out.println("Step 3 : Getting Contens .........");
                               // Loop through the list and get Content ID 
                                      for ( int m=0;m<aBatchFile.size();m++)

                                             {

                                              runGetContentID(prop,idcClient,userContext,dataBinder,aBatchFile.get(m));

                                             }
                                        
                                   long sleepingtime = aContenID.size()*1000;





                                  // Check the FolderID details of each content 

                                    //     System.out.println("Step 3 :  Folder Verification on Target  .........");

                                      //     runFFverification(prop,idcClient,userContext,dataBinder);

                                // Transfer the archiver collection 

                                              //    
                                                              // Thread.sleep(5000);                 //Sleeping for 5 secs 


                                                      System.out.println("Step 4 : Transferring to Target ...");

                                                     runTrasfer(prop,idcClient,userContext,dataBinder);

                                                            // Get the Transfer Status 
                                                                    
                                                               System.out.println("Sleeping for  " + sleepingtime + "  ms ");


                                                                  Thread.sleep(sleepingtime);                 //Sleeping for number of contents  
                                                   
                                                          System.out.println("Step 5 : Getting Archiver Transfer status  ...");

                                                                


                               int archiverCount=getArchiverStatus(prop,idcClient,userContext,dataBinder);

                        System.out.println("number of Contents in archiver collection is "+aContenID.size());
                        System.out.println("number of Contents are trasfered is "+archiverCount);
                                                            
                                                       System.out.println("Step 6 : Verifying  Transfer and Content Count  ...");

                                                     if ( archiverCount == aContenID.size())
                                                              deleteBatchList(prop,idcClient,userContext,dataBinder);
                                                      else 
                                                       System.out.println("There is issue with Archiver Count Not deleting the batch "); 

                                                        
                                                  System.out.println("Done!");

			
		} catch (IdcClientException ice){
			ice.printStackTrace();
		} catch (IOException ioe){
			ioe.printStackTrace();
		}catch(InterruptedException ex) {
                          Thread.currentThread().interrupt();
                 } 


	}  // end of main 





 public static void runExport   (Properties prop ,IdcClient idcClient , IdcContext userContext ,DataBinder dataBinder ) throws  IdcClientException


  {

                dataBinder.putLocal("IdcService", "EXPORT_ARCHIVE");
                dataBinder.putLocal("IDC_Name",prop.getProperty("IDC_Name"));
                dataBinder.putLocal("aArchiveName",prop.getProperty("aArchiveName"));
                dataBinder.putLocal("dataSource","RevisionIDs");
                dataBinder.putLocal("aDoExportTable","1");
                dataBinder.putLocal("forceLogin","1");
                dataBinder.putLocal("monitoredSubjects","collaborations");
                      

            ServiceResponse response = idcClient.sendRequest(userContext,dataBinder);

          





   }  // end of runExport 

 public static void runGetBatchFile   (Properties prop ,IdcClient idcClient , IdcContext userContext ,DataBinder dataBinder ) throws  IdcClientException


{

            dataBinder.putLocal("IdcService", "GET_BATCHFILES");
            dataBinder.putLocal("IDC_Name",prop.getProperty("IDC_Name"));
            dataBinder.putLocal("aArchiveName",prop.getProperty("aArchiveName"));

            ServiceResponse response = idcClient.sendRequest(userContext,dataBinder);
            
            
            DataBinder responseData = response.getResponseAsBinder();


            DataResultSet resultSet = responseData.getResultSet("BatchFiles");
           
                     if ( resultSet.getRows().size() > 0 )
                        {         
        
                               for (DataObject dataObject : resultSet.getRows ()) {


                                         aBatchFile.add(dataObject.get ("aBatchFile"));
                                                     }
                        }

                              else 
                             {
                                       System.out.println("empty list ..Nothing to do ...  exiting!");
                                        System.exit(0);
                              }
                             


} // end of runGetBatchFile



public static void  runGetContentID (Properties prop ,IdcClient idcClient , IdcContext userContext ,DataBinder dataBinder , String aBatchFile ) throws  IdcClientException


{
                
            dataBinder.putLocal("IdcService", "GET_BATCH_FILE_DOCUMENTS");
            dataBinder.putLocal("IDC_Name",prop.getProperty("IDC_Name"));
            dataBinder.putLocal("aArchiveName",prop.getProperty("aArchiveName"));
            dataBinder.putLocal("aBatchFile",aBatchFile);
            ServiceResponse response = idcClient.sendRequest(userContext,dataBinder);
              DataBinder responseData = response.getResponseAsBinder();
             DataResultSet resultSet = responseData.getResultSet("ExportResults");

                    for (DataObject dataObject : resultSet.getRows ()) {
       
    
                                          aContenID.add(dataObject.get ("dDocName") +"\t"+dataObject.get("dID"));
                                          System.out.println(dataObject.get ("dDocName"));
                                       //  System.out.println(dataObject.get ("dID"));
                                 }




} // end of runGetContentID
 

public static void runTrasfer(Properties prop ,IdcClient idcClient , IdcContext userContext ,DataBinder dataBinder ) throws  IdcClientException


{

            dataBinder.putLocal("IdcService", "TRANSFER_ARCHIVE");
            dataBinder.putLocal("IDC_Name",prop.getProperty("IDC_Name"));
            dataBinder.putLocal("aArchiveName",prop.getProperty("aArchiveName"));
            dataBinder.putLocal("aTargetArchive",prop.getProperty("aTargetArchive"));
                           

           
           
            ServiceResponse response = idcClient.sendRequest(userContext,dataBinder);



} // end of runTransfer 


public static int getArchiverStatus(Properties prop ,IdcClient idcClient , IdcContext userContext ,DataBinder dataBinder ) throws  IdcClientException


{

               int k =0;


             dataBinder.putLocal("IdcService", "GET_ARCHIVES");
            dataBinder.putLocal("IDC_Name",prop.getProperty("IDC_Name"));

           
            ServiceResponse response = idcClient.sendRequest(userContext,dataBinder);
                   
            DataBinder responseData = response.getResponseAsBinder();

            DataResultSet resultSet = responseData.getResultSet("ArchiveData");
           


                 String [] tokens = {""};
                String [] TotalTrans = {""};
                String [] count = {""};
      
                           


            for (DataObject dataObject : resultSet.getRows ()) {

                              if (dataObject.get("aArchiveName").equals(prop.getProperty("aArchiveName")))
                                       
                                             {                                   
                                                      //     System.out.println(dataObject.get ("aArchiveData"));

                                         tokens = dataObject.get ("aArchiveData").split("\\n+");
                                                           // System.out.println(dataObject.get ("aArchiveData"));
                                  

                                              break;
                                            }
                           

            } 



for ( String st  : tokens  )

{


if ( st.contains("aTotalTransferedOut="))
       {
                  TotalTrans = st.split("\\=+");
                     
                 // System.out.println(TotalTrans[1]);
                      
                  count = TotalTrans[1].split("\\s+");
                  //       System.out.println(count[0]);
              k = Integer.valueOf(count[0]);
                       break;         
       }


}


return k;


} // end of getArchiver Status 

 public static void deleteBatchList   (Properties prop ,IdcClient idcClient , IdcContext userContext ,DataBinder dataBinder ) throws  IdcClientException


{

            dataBinder.putLocal("IdcService", "DELETE_BATCH_FILE");
            dataBinder.putLocal("IDC_Name",prop.getProperty("IDC_Name"));
            dataBinder.putLocal("aArchiveName",prop.getProperty("aArchiveName"));

                  System.out.println("Step 7 : Deleting Batchlist .......");

                                                       for ( int m=0;m<aBatchFile.size();m++)

                                             {

            dataBinder.putLocal("aBatchFile",aBatchFile.get(m));       

            ServiceResponse response = idcClient.sendRequest(userContext,dataBinder);
            
     
                      
                                           }


} // end of  deleteBatchList

 public static void runFFverification   (Properties prop ,IdcClient idcClient , IdcContext userContext ,DataBinder dataBinder ) throws  IdcClientException


{

            dataBinder.putLocal("IdcService", "DOC_INFO");

                  for ( int k=0;k<aContenID.size();k++)

                          {
                                 String [] token = aContenID.get(k).split("\\t+");

                                 dataBinder.putLocal("dID",token[1]);

                                 ServiceResponse response = idcClient.sendRequest(userContext,dataBinder);
            
            
                                  DataBinder responseData = response.getResponseAsBinder();
                                       
                                         String FolderPath=responseData.getLocal("fParentPath");

                                     if ( FolderPath != null )
                                           
                              {
                                                                  
                           boolean folderstatus = VerifyFFinTarger.verifyFFinTargetMethod(FolderPath);
                                                 if (folderstatus)
                                                 {
                                                         System.out.println ( "Content  "+token[0]+" belongs to "+FolderPath + " is found on target system ");
                                                      
                                                  }
                                     
                                                else 

                                                  {
                                                            System.out.println ( "Content  "+token[0]+" belongs to "+FolderPath + " is NOT found on target system , exiting..");  
                                                                   System.exit(0);

                                                   }
                               } // end of Folderpath check if 


                                else 

                                 {
                                           System.out.println ( "Content  "+token[0]+" is not  belongs to any folder  ");        

                                  }      
    
                          } // end of For Loop 


} // end of runFFverification



} // end of class 
