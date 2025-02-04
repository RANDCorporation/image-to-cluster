/*
 * Autopsy Forensic Browser
 *
 * Copyright 2012 Basis Technology Corp.
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

import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.io.File;
import java.util.*;
import javax.swing.JFileChooser;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import javax.swing.JPanel;
import javax.swing.filechooser.FileFilter;
import org.apache.commons.lang3.StringUtils;

import org.openide.util.NbBundle;
import org.sleuthkit.autopsy.corecomponentinterfaces.DataSourceProcessor;
import org.sleuthkit.autopsy.coreutils.ModuleSettings;
import org.sleuthkit.autopsy.coreutils.MessageNotifyUtil;
import java.util.logging.Level;
import org.sleuthkit.autopsy.casemodule.Case;
import org.sleuthkit.autopsy.coreutils.Logger;
import org.sleuthkit.autopsy.coreutils.PathValidator;
import org.sleuthkit.autopsy.coreutils.DriveUtils;

/**
 * ImageTypePanel for adding an image file such as .img, .E0x, .00x, etc.
 */
public class ImageFileToClusterPanel extends JPanel implements DocumentListener {
    
    private final String PROP_LASTIMAGE_PATH = "LBL_LastImage_PATH"; //NON-NLS
    private static final Logger logger = Logger.getLogger(ImageFileToClusterPanel.class.getName());
    private PropertyChangeSupport pcs = null;
    private JFileChooser fc = new JFileChooser();
    private File[] objFiles_;
    
    // Externally supplied name is used to store settings 
    private String contextName;

    /**
     * Creates new form ImageFilePanel
     * @param context a string context name used to read/store last used settings
     * @param fileChooserFilters a list of filters to be used with the FileChooser
     */
    private ImageFileToClusterPanel(String context, List<FileFilter> fileChooserFilters) {
        initComponents();
        fc.setDragEnabled(false);
        fc.setFileSelectionMode(JFileChooser.FILES_ONLY);
        fc.setMultiSelectionEnabled(true);
        
        errorLabel.setVisible(false);
        
        boolean firstFilter = true;
        for (FileFilter filter: fileChooserFilters ) {
            if (firstFilter) {  // set the first on the list as the default selection
                fc.setFileFilter(filter);
                firstFilter = false;
            } 
            else {
                fc.addChoosableFileFilter(filter);
            }
        }
        
        this.contextName = context;
        pcs = new PropertyChangeSupport(this);
        
        createTimeZoneList();       
    }
    
    /**
     * Creates and returns an instance of a ImageFilePanel.
     */
    public static synchronized ImageFileToClusterPanel createInstance(String context, List<FileFilter> fileChooserFilters) {
        
       ImageFileToClusterPanel instance = new ImageFileToClusterPanel(context, fileChooserFilters );
       
       instance.postInit();
        
       return instance;
    }

    //post-constructor initialization to properly initialize listener support
    //without leaking references of uninitialized objects
    private void postInit() {
        pathTextField.getDocument().addDocumentListener(this);
    }
    

    /**
     * This method is called from within the constructor to initialize the form.
     * WARNING: Do NOT modify this code. The content of this method is always
     * regenerated by the Form Editor.
     */
    @SuppressWarnings("unchecked")
    // <editor-fold defaultstate="collapsed" desc="Generated Code">//GEN-BEGIN:initComponents
    private void initComponents() {

        pathLabel = new javax.swing.JLabel();
        browseButton = new javax.swing.JButton();
        pathTextField = new javax.swing.JTextField();
        timeZoneLabel = new javax.swing.JLabel();
        timeZoneComboBox = new javax.swing.JComboBox<String>();
        noFatOrphansCheckbox = new javax.swing.JCheckBox();
        descLabel = new javax.swing.JLabel();
        errorLabel = new javax.swing.JLabel();

        setMinimumSize(new java.awt.Dimension(0, 65));
        setPreferredSize(new java.awt.Dimension(403, 65));

        org.openide.awt.Mnemonics.setLocalizedText(pathLabel, org.openide.util.NbBundle.getMessage(ImageFileToClusterPanel.class, "ImageFileToClusterPanel.pathLabel.text")); // NOI18N

        org.openide.awt.Mnemonics.setLocalizedText(browseButton, org.openide.util.NbBundle.getMessage(ImageFileToClusterPanel.class, "ImageFileToClusterPanel.browseButton.text")); // NOI18N
        browseButton.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                browseButtonActionPerformed(evt);
            }
        });

        pathTextField.setText(org.openide.util.NbBundle.getMessage(ImageFileToClusterPanel.class, "ImageFileToClusterPanel.pathTextField.text")); // NOI18N

        org.openide.awt.Mnemonics.setLocalizedText(timeZoneLabel, org.openide.util.NbBundle.getMessage(ImageFileToClusterPanel.class, "ImageFileToClusterPanel.timeZoneLabel.text")); // NOI18N

        timeZoneComboBox.setMaximumRowCount(30);

        org.openide.awt.Mnemonics.setLocalizedText(noFatOrphansCheckbox, org.openide.util.NbBundle.getMessage(ImageFileToClusterPanel.class, "ImageFileToClusterPanel.noFatOrphansCheckbox.text")); // NOI18N
        noFatOrphansCheckbox.setToolTipText(org.openide.util.NbBundle.getMessage(ImageFileToClusterPanel.class, "ImageFileToClusterPanel.noFatOrphansCheckbox.toolTipText")); // NOI18N

        org.openide.awt.Mnemonics.setLocalizedText(descLabel, org.openide.util.NbBundle.getMessage(ImageFileToClusterPanel.class, "ImageFileToClusterPanel.descLabel.text")); // NOI18N

        errorLabel.setForeground(new java.awt.Color(255, 0, 0));
        org.openide.awt.Mnemonics.setLocalizedText(errorLabel, org.openide.util.NbBundle.getMessage(ImageFileToClusterPanel.class, "ImageFileToClusterPanel.errorLabel.text")); // NOI18N

        javax.swing.GroupLayout layout = new javax.swing.GroupLayout(this);
        this.setLayout(layout);
        layout.setHorizontalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(layout.createSequentialGroup()
                .addComponent(pathTextField)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
                .addComponent(browseButton)
                .addGap(2, 2, 2))
            .addGroup(layout.createSequentialGroup()
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addGroup(layout.createSequentialGroup()
                        .addComponent(timeZoneLabel)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
                        .addComponent(timeZoneComboBox, javax.swing.GroupLayout.PREFERRED_SIZE, 215, javax.swing.GroupLayout.PREFERRED_SIZE))
                    .addComponent(pathLabel)
                    .addComponent(noFatOrphansCheckbox)
                    .addGroup(layout.createSequentialGroup()
                        .addGap(21, 21, 21)
                        .addComponent(descLabel))
                    .addComponent(errorLabel))
                .addGap(0, 20, Short.MAX_VALUE))
        );
        layout.setVerticalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(layout.createSequentialGroup()
                .addComponent(pathLabel)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(browseButton)
                    .addComponent(pathTextField, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addGap(3, 3, 3)
                .addComponent(errorLabel)
                .addGap(1, 1, 1)
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(timeZoneLabel)
                    .addComponent(timeZoneComboBox, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
                .addComponent(noFatOrphansCheckbox)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(descLabel)
                .addContainerGap(javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
        );
    }// </editor-fold>//GEN-END:initComponents

    private void browseButtonActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_browseButtonActionPerformed
        String oldText = pathTextField.getText();
        // set the current directory of the FileChooser if the ImagePath Field is valid
        File currentDir = new File(oldText);
        if (currentDir.exists()) {
            fc.setCurrentDirectory(currentDir);
        }

        int retval = fc.showOpenDialog(this);
        if (retval == JFileChooser.APPROVE_OPTION) {
            objFiles_ = fc.getSelectedFiles();
            pathTextField.setText(objFiles_[0].getPath());
        }
        
      
            
        try {
            pcs.firePropertyChange(DataSourceProcessor.DSP_PANEL_EVENT.FOCUS_NEXT.toString(), false, true);
        }
        catch (Exception e) {
            logger.log(Level.SEVERE, "ImageFilePanel listener threw exception", e); //NON-NLS
            MessageNotifyUtil.Notify.show(NbBundle.getMessage(this.getClass(), "ImageFilePanel.moduleErr"),
                                          NbBundle.getMessage(this.getClass(), "ImageFilePanel.moduleErr.msg"),
                                          MessageNotifyUtil.MessageType.ERROR);
        }
    }//GEN-LAST:event_browseButtonActionPerformed

    // Variables declaration - do not modify//GEN-BEGIN:variables
    private javax.swing.JButton browseButton;
    private javax.swing.JLabel descLabel;
    private javax.swing.JLabel errorLabel;
    private javax.swing.JCheckBox noFatOrphansCheckbox;
    private javax.swing.JLabel pathLabel;
    private javax.swing.JTextField pathTextField;
    private javax.swing.JComboBox<String> timeZoneComboBox;
    private javax.swing.JLabel timeZoneLabel;
    // End of variables declaration//GEN-END:variables
        
    /**
     * Get the path of the user selected image.
     * @return the image path
     */
    public String getContentPaths() {
        return pathTextField.getText();
    }

    public String[] getContentPathArray() {
        ArrayList<String> objPaths = new ArrayList<String>();
        for( File objFile : objFiles_ ){
            objPaths.add( objFile.getPath());
        }
        return objPaths.toArray(new String[objPaths.size()]);
    }

    public File[] getFiles(){
        return objFiles_;
    }

    /**
     * Set the path of the image file.
     */
    public void setContentPath(String s) {
        pathTextField.setText(s);
    }

    public String getTimeZone() {
        String tz = timeZoneComboBox.getSelectedItem().toString();
        return tz.substring(tz.indexOf(")") + 2).trim();
        
    }
    
    public boolean getNoFatOrphans() {
        return noFatOrphansCheckbox.isSelected();
    }
     
    

    public void reset() {
        //reset the UI elements to default 
        pathTextField.setText(null);  
    }
    
    /**
     * Should we enable the next button of the wizard?
     * @return true if a proper image has been selected, false otherwise
     */
    public boolean validatePanel() {
        errorLabel.setVisible(false);
        String path = getContentPaths();
        if (StringUtils.isBlank(path)) {
            return false;
        }

        // display warning if there is one (but don't disable "next" button)
        if (false == PathValidator.isValid(path, Case.getCurrentCase().getCaseType())) {
            errorLabel.setVisible(true);
            errorLabel.setText("Bundle.DataSourceOnCDriveError_text()");
        }

        return new File(path).isFile()
                || DriveUtils.isPhysicalDrive(path)
                || DriveUtils.isPartition(path);
    }
    
    /**
     * Validates path to selected data source and displays warning if it is invalid.
     * @param path Absolute path to the selected data source
     */
    private void warnIfPathIsInvalid(String path){                      
        if (!PathValidator.isValid(path, Case.getCurrentCase().getCaseType())) {
            errorLabel.setVisible(true);
            errorLabel.setText(NbBundle.getMessage(this.getClass(), "DataSourceOnCDriveError.text"));
        }
    }
    
    public void storeSettings() {
        String imagePathName = getContentPaths();
        if (null != imagePathName ) {
            String imagePath = imagePathName.substring(0, imagePathName.lastIndexOf(File.separator) + 1);
            ModuleSettings.setConfigSetting(contextName, PROP_LASTIMAGE_PATH, imagePath);
        }
    }
    
    public void readSettings() {
        String lastImagePath = ModuleSettings.getConfigSetting(contextName, PROP_LASTIMAGE_PATH);
        if (null != lastImagePath) {
            if (!lastImagePath.isEmpty())
                 pathTextField.setText(lastImagePath);  
        }  
    }
    /**
     * Creates the drop down list for the time zones and then makes the local
     * machine time zone to be selected.
     */
     public void createTimeZoneList() {
        // load and add all timezone
        String[] ids = SimpleTimeZone.getAvailableIDs();
        for (String id : ids) {
            TimeZone zone = TimeZone.getTimeZone(id);
            int offset = zone.getRawOffset() / 1000;
            int hour = offset / 3600;
            int minutes = (offset % 3600) / 60;
            String item = String.format("(GMT%+d:%02d) %s", hour, minutes, id); //NON-NLS

            /*
             * DateFormat dfm = new SimpleDateFormat("z");
             * dfm.setTimeZone(zone); boolean hasDaylight =
             * zone.useDaylightTime(); String first = dfm.format(new Date(2010,
             * 1, 1)); String second = dfm.format(new Date(2011, 6, 6)); int mid
             * = hour * -1; String result = first + Integer.toString(mid);
             * if(hasDaylight){ result = result + second; }
             * timeZoneComboBox.addItem(item + " (" + result + ")");
             */
            timeZoneComboBox.addItem(item);
        }
        // get the current timezone
        TimeZone thisTimeZone = Calendar.getInstance().getTimeZone();
        int thisOffset = thisTimeZone.getRawOffset() / 1000;
        int thisHour = thisOffset / 3600;
        int thisMinutes = (thisOffset % 3600) / 60;
        String formatted = String.format("(GMT%+d:%02d) %s", thisHour, thisMinutes, thisTimeZone.getID()); //NON-NLS

        // set the selected timezone
        timeZoneComboBox.setSelectedItem(formatted);
    }
    /**
     * Update functions are called by the pathTextField which has this set
     * as it's DocumentEventListener. Each update function fires a property change
     * to be caught by the parent panel.
     * @param e the event, which is ignored
     */
    @Override
    public void insertUpdate(DocumentEvent e) {
        
        try {
            pcs.firePropertyChange(DataSourceProcessor.DSP_PANEL_EVENT.UPDATE_UI.toString(), false, true);
        }
        catch (Exception ee) {
            logger.log(Level.SEVERE, "ImageFilePanel listener threw exception", ee); //NON-NLS
            MessageNotifyUtil.Notify.show(NbBundle.getMessage(this.getClass(), "ImageFilePanel.moduleErr"),
                                          NbBundle.getMessage(this.getClass(), "ImageFilePanel.moduleErr.msg"),
                                          MessageNotifyUtil.MessageType.ERROR);
        }
    }

    @Override
    public void removeUpdate(DocumentEvent e) { 
        try {
             pcs.firePropertyChange(DataSourceProcessor.DSP_PANEL_EVENT.UPDATE_UI.toString(), false, true);
        }
        catch (Exception ee) {
            logger.log(Level.SEVERE, "ImageFilePanel listener threw exception", ee); //NON-NLS
            MessageNotifyUtil.Notify.show(NbBundle.getMessage(this.getClass(), "ImageFilePanel.moduleErr"),
                                          NbBundle.getMessage(this.getClass(), "ImageFilePanel.moduleErr.msg"),
                                          MessageNotifyUtil.MessageType.ERROR);
        }
    }

    @Override
    public void changedUpdate(DocumentEvent e) {
        
        try {
            pcs.firePropertyChange(DataSourceProcessor.DSP_PANEL_EVENT.UPDATE_UI.toString(), false, true);
        }
        catch (Exception ee) {
            logger.log(Level.SEVERE, "ImageFilePanel listener threw exception", ee); //NON-NLS
            MessageNotifyUtil.Notify.show(NbBundle.getMessage(this.getClass(), "ImageFilePanel.moduleErr"),
                                          NbBundle.getMessage(this.getClass(), "ImageFilePanel.moduleErr.msg"),
                                          MessageNotifyUtil.MessageType.ERROR);
        }
    }
    
    /**
     * Set the focus to the pathTextField.
     */
    public void select() {
        pathTextField.requestFocusInWindow();
    }
    
    
    @Override
    public synchronized void addPropertyChangeListener(PropertyChangeListener pcl) {
	super.addPropertyChangeListener(pcl);

	if (pcs == null) {
	    pcs = new PropertyChangeSupport(this);
	}

        pcs.addPropertyChangeListener(pcl);
    }
    
    @Override
    public void removePropertyChangeListener(PropertyChangeListener pcl) {
	super.removePropertyChangeListener(pcl);

        pcs.removePropertyChangeListener(pcl);
    }

}
