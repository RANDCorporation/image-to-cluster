/*
 * Autopsy Forensic Browser
 *
 * Copyright 2013-2014 Basis Technology Corp.
 * Contact: carrier <at> sleuthkit <dot> org
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.rand.image2cluster;

import javax.swing.JPanel;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import javax.swing.filechooser.FileFilter;
import org.openide.util.Exceptions;

import org.openide.util.NbBundle;
import org.openide.util.lookup.ServiceProvider;
import org.openide.util.lookup.ServiceProviders;
import org.sleuthkit.autopsy.casemodule.GeneralFilter;
import org.sleuthkit.autopsy.coreutils.Logger;
import org.sleuthkit.autopsy.corecomponentinterfaces.DataSourceProcessorProgressMonitor;
import org.sleuthkit.autopsy.corecomponentinterfaces.DataSourceProcessorCallback;
import org.sleuthkit.autopsy.corecomponentinterfaces.DataSourceProcessor;
import org.sleuthkit.datamodel.Content;
import org.sleuthkit.autopsy.casemodule.Case;
import org.sleuthkit.autopsy.datasourceprocessors.AutoIngestDataSourceProcessor;
import org.sleuthkit.datamodel.TskCoreException;

/**
 * Image data source processor.
 * Handles the addition of  "disk images" to Autopsy.
 * 
 * An instance of this class is created via the Netbeans Lookup() method.
 * 
 */
@ServiceProviders(value = {
    @ServiceProvider(service = DataSourceProcessor.class)}
)
public class ImageFileToClusterDSProcessor implements DataSourceProcessor {
  
    static final Logger logger = Logger.getLogger(ImageFileToClusterDSProcessor.class.getName());
    
    // Data source type handled by this processor
    private final static String dsType = NbBundle.getMessage(ImageFileToClusterDSProcessor.class, "ImageFileToClusterDSProcessor.dsType.text");
    
    // The Config UI panel that plugins into the Choose Data Source Wizard
    private final ImageFileToClusterPanel imageFilePanel;
    
    // The Background task that does the actual work of adding the image 
    private AddImageTask addImageTask;
   
    // The Background task that does the actual work of adding the image 
    private AddLocalFilesToClusterTask addToClusterTask;
    
    // true of cancelled by the caller
    private boolean cancelled = false;
    
    DataSourceProcessorCallback callbackObj = null;
    
    // set to TRUE if the image options have been set via API and config Jpanel should be ignored
    private boolean imageOptionsSet = false;
   
    
    // image options
    private String imagePath;
    private String timeZone;
    private boolean noFatOrphans;
     
    static final GeneralFilter rawFilter = new GeneralFilter(GeneralFilter.RAW_IMAGE_EXTS, GeneralFilter.RAW_IMAGE_DESC);
    static final GeneralFilter encaseFilter = new GeneralFilter(GeneralFilter.ENCASE_IMAGE_EXTS, GeneralFilter.ENCASE_IMAGE_DESC);
    
    static final List<String> allExt = new ArrayList<>();
    static {
        allExt.addAll(GeneralFilter.RAW_IMAGE_EXTS);
        allExt.addAll(GeneralFilter.ENCASE_IMAGE_EXTS);
    }
    static final String allDesc = NbBundle.getMessage(ImageFileToClusterDSProcessor.class, "ImageFileToClusterDSProcessor.allDesc.text");
    static final GeneralFilter allFilter = new GeneralFilter(allExt, allDesc);
    
    static final List<FileFilter> filtersList = new ArrayList<>();
    
    static {
        filtersList.add(allFilter);
        filtersList.add(rawFilter);
        filtersList.add(encaseFilter);
    }
    
    /*
     * A no argument constructor is required for the NM lookup() method to create an object
     */
    public ImageFileToClusterDSProcessor() {
        
        // Create the config panel
        imageFilePanel =  ImageFileToClusterPanel.createInstance(ImageFileToClusterDSProcessor.class.getName(), filtersList);
        
    }
    
    // this static method is used by the wizard to determine dsp type for 'core' data source processors
    public static String getType() {
        return dsType;
    }
     
    /**
     * Returns the Data source type (string) handled by this DSP
     *
     * @return String the data source type
     **/ 
    @Override
    public String getDataSourceType() {
        return dsType;
    }
            
    /**
     * Returns the JPanel for collecting the Data source information
     *
     * @return JPanel the config panel 
     **/ 
   @Override
    public JPanel getPanel() {

       imageFilePanel.readSettings();
       imageFilePanel.select();
       
       return imageFilePanel;
   }
    
    /**
     * Validates the data collected by the JPanel
     *
     * @return String returns NULL if success, error string if there is any errors  
     **/  
   @Override
   public boolean isPanelValid() {
       
        return imageFilePanel.validatePanel(); 
   }
   
    /**
     * Runs the data source processor.
     * This must kick off processing the data source in background
     *
     * @param progressMonitor Progress monitor to report progress during processing 
     * @param cbObj callback to call when processing is done.
     **/    
  @Override
  public void run(DataSourceProcessorProgressMonitor progressMonitor, final DataSourceProcessorCallback cbObj) {
      
      callbackObj = cbObj;
      cancelled = false;

      DataSourceProcessorCallback cbObjOverride = new DataSourceProcessorCallback() {
          @Override
          public void doneEDT(DataSourceProcessorCallback.DataSourceProcessorResult result, List<String> errList, List<Content> contents) {
              //dataSourceProcessorDone(dataSourceId, result, errList, contents);
              //pass an empty content list to the old callback (so that it doesn't run the ingest modules)
              // but still notify the GUI that we finished loading
              cbObj.doneEDT(result, errList, new ArrayList<Content> ());
              //We don't have the original dataSourceId given to the GUI, so we'll just generate a new one—this is only used by the GUI anyway
              new Thread(() -> {
                  UUID dataSourceId = UUID.randomUUID();
                  if (!contents.isEmpty()) {
                      Case.getCurrentCase().notifyDataSourceAdded(contents.get(0), dataSourceId);
                  } else {
                      Case.getCurrentCase().notifyFailedAddingDataSource(dataSourceId);
                  }
              }).start();
          }
      };


      
      if (!imageOptionsSet)
      {
          //tell the panel to save the current settings
          imageFilePanel.storeSettings();
          
          // get the image options from the panel
          imagePath = imageFilePanel.getContentPaths();
          timeZone = imageFilePanel.getTimeZone();
          noFatOrphans = imageFilePanel.getNoFatOrphans(); 
      }

      String[] imagePaths = {imagePath};
      imagePath = imageFilePanel.getContentPaths();

      String deviceId = UUID.randomUUID().toString();
      // Send callback, so Autopy GUI updates
      addImageTask = new AddImageTask(deviceId, imagePaths, timeZone, noFatOrphans, progressMonitor, cbObjOverride);
      Thread addImageThread = new Thread(addImageTask);
      addImageThread.start();

      // Wait for the add image task to complete,
      // before starting the send image to cluster task.
      try {
          addImageThread.join();
      } catch (InterruptedException ex) {
          Exceptions.printStackTrace(ex);
      }
      try {
          //Case.getCurrentCase().getSleuthkitCase().clearImagePathsAndSizes() TODO: Requires our version of sleuthkit
      } catch (Exception objException ){
          objException.printStackTrace();
      }
      // TODO: Currently only uses the first image path
      // to send to cluster, ignore callback
      // TODO: put this back in by reconciling the progress monitors
      addToClusterTask = new AddLocalFilesToClusterTask(imagePath, "topic_case_" + Case.getCurrentCase().getName(), progressMonitor, null);
      new Thread(addToClusterTask).start();
  }
  
    /**
     * Cancel the data source processing
     **/    
  @Override
  public void cancel() {
      
      cancelled = true;
      addImageTask.cancelTask();
      addToClusterTask.cancelTask();
  }
  
  /**
   * Reset the data source processor
   **/    
  @Override
  public void reset() {
      
     // reset the config panel
     imageFilePanel.reset();
    
     // reset state 
     imageOptionsSet = false;
     imagePath = null;
     timeZone = null;
     noFatOrphans = false;
  }
  
  /**
   * Sets the data source options externally.
   * To be used by a client that does not have a UI and does not use the JPanel to 
   * collect this information from a user.
   * 
   * @param imgPath path to thew image or first image
   * @param tz timeZone 
   * @param noFat whether to parse FAT orphans
   **/ 
  public void setDataSourceOptions(String imgPath, String tz, boolean noFat) {
      
    this.imagePath = imgPath;
    this.timeZone  = tz;
    this.noFatOrphans = noFat;
      
    imageOptionsSet = true;
      
  }
}
