<?xml version="1.0" encoding="utf-8"?>
<!-- $Id: S4paDIF102HTML.xsl,v 1.1 2015/12/11 13:36:00 glei Exp $ -->
<!-- -@@@ S4PA, Version $Name:  $ -->
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">

<!-- This XSLT transform DIF10 XML files for display -->
<xsl:output method="html" encoding="UTF-8" indent="yes" />
    
<!-- Provide HTML header and start examining XML tags -->
   <xsl:template match="/">
       <html xmlns="http://www.w3.org/1999/xhtml">
           <head><title>DIF</title></head>
           <body>
               <xsl:apply-templates />
           </body>
       </html>
   </xsl:template>

<!-- Make tables under DIF tag, starting with Entry table -->
   <xsl:template match="DIF">
    <h3 style="margin-bottom: 0;"><strong>Entry</strong></h3>
    <blockquote style="margin-top: 0;">
      <table width="800" border="1">
        <tr>
          <td width="120"><strong>Short_Name</strong></td>
          <td><xsl:value-of select="Entry_ID/Short_Name" /></td>
        </tr>
        <tr>
          <td><strong>Version</strong></td>
          <td><xsl:value-of select="Entry_ID/Version" /></td>
        </tr>
        <tr>
          <td><strong>Entry_Title</strong></td>
          <td><xsl:value-of select="Entry_Title" /></td>
        </tr>
      </table>
    </blockquote>
    <br/>

<!-- Dataset Citation table -->
    <h3 style="margin-bottom: 0;"><strong>Dataset Citation</strong></h3>
    <blockquote style="margin-top: 0;">
      <table width="800" border="1">
        <tr>
          <td width="150"><strong>Creator</strong></td>
          <td><xsl:value-of select="Dataset_Citation/Dataset_Creator" /></td>
        </tr>
        <tr>
          <td><strong>Title</strong></td>
          <td><xsl:value-of select="Dataset_Citation/Dataset_Title" /></td>
        </tr>
        <xsl:if test="Dataset_Citation/Dataset_Series_Name">
          <tr>
            <td><strong>Series Name</strong></td>
            <td><xsl:value-of select="Dataset_Citation/Dataset_Series_Name" /></td>
          </tr>
        </xsl:if>
        <xsl:if test="Dataset_Citation/Dataset_Release_Date">
          <tr>
            <td><strong>Release Date</strong></td>
            <td><xsl:value-of select="Dataset_Citation/Dataset_Release_Date" /></td>
          </tr>
        </xsl:if>
        <xsl:if test="Dataset_Citation/Dataset_Release_Place">
          <tr>
            <td><strong>Release Place</strong></td>
            <td><xsl:value-of select="Dataset_Citation/Dataset_Release_Place" /></td>
          </tr>
        </xsl:if>
        <xsl:if test="Dataset_Citation/Dataset_Publisher">
          <tr>
            <td><strong>Publisher</strong></td>
            <td><xsl:value-of select="Dataset_Citation/Dataset_Publisher" /></td>
          </tr>
        </xsl:if>
        <xsl:if test="Dataset_Citation/Version">
          <tr>
            <td><strong>Version</strong></td>
            <td><xsl:value-of select="Dataset_Citation/Version" /></td>
          </tr>
        </xsl:if>
        <xsl:if test="Dataset_Citation/Issue_Identification">
          <tr>
            <td><strong>Issue</strong></td>
            <td><xsl:value-of select="Dataset_Citation/Issue_Identification" /></td>
          </tr>
        </xsl:if>
        <xsl:if test="Dataset_Citation/Data_Presentation_Form">
          <tr>
            <td><strong>Presentation Form</strong></td>
            <td><xsl:value-of select="Dataset_Citation/Data_Presentation_Form" /></td>
          </tr>
        </xsl:if>
        <xsl:if test="Dataset_Citation/Other_Citation_Details">
          <tr>
            <td><strong>Citation Details</strong></td>
            <td>
              <xsl:call-template name="CopyWithHyperLink">
                <xsl:with-param name="link" select="Dataset_Citation/Other_Citation_Details" />
              </xsl:call-template>
            </td>
          </tr>
        </xsl:if>
        <xsl:if test="Dataset_Citation/Online_Resource">
          <tr>
            <td><strong>Online Resource</strong></td>
            <td>
              <xsl:element name="a"><xsl:attribute name="href"><xsl:value-of select="Dataset_Citation/Online_Resource" /></xsl:attribute><xsl:value-of select="Dataset_Citation/Online_Resource" /></xsl:element>
            </td>
          </tr>
        </xsl:if>
      </table>
    </blockquote>
    <br/>

<!-- Personnel Table -->
    <h3 style="margin-bottom: 0;"><strong>Personnel</strong></h3>
    <blockquote style="margin-top: 0;">
      <xsl:for-each select="Personnel">
        <table width="800" border="1">
          <tr>
            <td width="150"><strong>Role</strong></td>
            <td><xsl:value-of select="Role" /><xsl:text> </xsl:text></td>
          </tr>
          <tr>
            <td><strong>Name</strong></td>
            <td>
              <xsl:if test="Contact_Person/First_Name">
                <xsl:value-of select="Contact_Person/First_Name" /><xsl:text> </xsl:text>
              </xsl:if>
              <xsl:if test="Middle_Name">
                <xsl:value-of select="Contact_Person/Middle_Name" /><xsl:text> </xsl:text>
              </xsl:if>
              <xsl:value-of select="Contact_Person/Last_Name" />
            </td>
          </tr>
          <xsl:if test="Contact_Person/Email">
	    <tr>
              <td><strong>Email</strong></td>
              <td>
                <xsl:element name="a"><xsl:attribute name="href"><xsl:text>mailto:</xsl:text><xsl:value-of select="Email" /></xsl:attribute><xsl:value-of select="Contact_Person/Email" /></xsl:element>
              </td>
	    </tr>
          </xsl:if>
          <xsl:for-each select="Contact_Person/Phone">
          <xsl:if test="Type = 'Telephone'">
            <tr>
              <td><strong>Phone</strong></td>
              <td>
                <xsl:value-of select="Number" />
              </td>
            </tr>
          </xsl:if>
          <xsl:if test="Type = 'Fax'">
            <tr>
              <td><strong>Fax</strong></td>
              <td>
                <xsl:value-of select="Number" />
              </td>
            </tr>
          </xsl:if>
          </xsl:for-each>
          <xsl:if test="Contact_Person/Address">
	    <tr>
              <td><strong>Contact Address</strong></td>
              <td>
                <xsl:if test="Contact_Person/Address/Street_Address">
                  <xsl:for-each select="Contact_Person/Address/Street_Address">
                    <xsl:call-template name="CopyWithLineBreaks">
                      <xsl:with-param name="string" select="." />
                    </xsl:call-template><br/>
                  </xsl:for-each>
                </xsl:if>
                <xsl:if test="Contact_Person/Address/City">
                  <xsl:value-of select="Contact_Person/Address/City" /><xsl:text>, </xsl:text>
                </xsl:if>
                <xsl:if test="Contact_Person/Address/State_Province">
                  <xsl:value-of select="Contact_Person/Address/State_Province" /><xsl:text>, </xsl:text>
                </xsl:if>
                <xsl:if test="Contact_Person/Address/Postal_Code">
                  <xsl:value-of select="Contact_Person/Address/Postal_Code" /><xsl:text>, </xsl:text>
                </xsl:if>
                <xsl:if test="Contact_Person/Address/Country">
                  <xsl:value-of select="Contact_Person/Address/Country" />
                </xsl:if>
              </td>
  	    </tr>
          </xsl:if>
        </table>
        <br/>
      </xsl:for-each>
    </blockquote>
  
<!-- Parameters table -->
    <h3 style="margin-bottom: 0;"><strong>Parameters</strong></h3>
    <blockquote style="margin-top: 0;">
      <xsl:for-each select="Science_Keywords">
        <table width="800" border="1">
          <tr>
            <td width="150"><strong>Category</strong></td>
            <td><xsl:value-of select="Category" /></td>
          </tr>
          <tr>
            <td><strong>Topic</strong></td>
            <td><xsl:value-of select="Topic" /></td>
          </tr>
          <tr>
            <td><strong>Term</strong></td>
            <td><xsl:value-of select="Term" /></td>
          </tr>
          <tr>
            <td><strong>Variable Level 1</strong></td>
            <td><xsl:value-of select="Variable_Level_1" /></td>
          </tr>
        </table>
        <br/>
      </xsl:for-each>	
      <table width="800" border="1">
        <tr>
          <td><strong>ISO Topic Category</strong></td>
          <td><xsl:value-of select="ISO_Topic_Category" /></td>
        </tr>
        <tr>
          <td><strong>Keywords</strong></td>
          <td>
            <xsl:for-each select="Ancillary_Keyword">
              <xsl:value-of select="current()" /><xsl:text> -- </xsl:text>
            </xsl:for-each>
          </td>
        </tr>
      </table>
    </blockquote>
    <br/>

<!-- Instrument table; optional depending on existence of Instrument tag -->
    <xsl:if test="Platform/Instrument">
      <h3 style="margin-bottom: 0;"><strong>Instrument</strong></h3>
      <blockquote style="margin-top: 0;">
      <xsl:for-each select="Platform/Instrument">
        <table width="800" border="1">
          <tr>
            <td width="100"><strong>Short Name</strong></td>
            <td><xsl:value-of select="Short_Name" /></td>
          </tr>
          <tr>
            <td><strong>Long Name</strong></td>
            <td><xsl:value-of select="Long_Name" /></td>
          </tr>
        </table>
        <br/>
      </xsl:for-each>
      </blockquote>
    </xsl:if>

<!-- Platform table; optional depending on existence of Platform tag -->
    <xsl:if test="Platform">
      <h3 style="margin-bottom: 0;"><strong>Platform</strong></h3>
      <blockquote style="margin-top: 0;">
      <xsl:for-each select="Platform">
        <table width="800" border="1">
          <tr>
            <td width="100"><strong>Short Name</strong></td>
            <td><xsl:value-of select="Short_Name" /></td>
          </tr>
          <tr>
            <td><strong>Long Name</strong></td>
            <td><xsl:value-of select="Long_Name" /></td>
          </tr>
        </table>
        <br/>
      </xsl:for-each>
      </blockquote>
    </xsl:if>

<!-- Temporal Information table; using data under Temporal_Coverage,
     Data_Set_Progress, and Data_Resolution tags -->
    <h3 style="margin-bottom: 0;"><strong>Temporal Information</strong></h3>
    <blockquote style="margin-top: 0;">
      <table width="800" border="1">
        <xsl:if test="Temporal_Coverage/Range_DateTime/Beginning_Date_Time">
          <tr>
            <td width="120"><strong>Start Date</strong></td>
            <td><xsl:value-of select="Temporal_Coverage/Range_DateTime/Beginning_Date_Time" /></td>
          </tr>
        </xsl:if>
        <xsl:if test="Temporal_Coverage/Range_DateTime/Ending_Date_Time">
          <tr>
            <td><strong>Stop Date</strong></td>
            <td><xsl:value-of select="Temporal_Coverage/Range_DateTime/Ending_Date_Time" /></td>
          </tr>
        </xsl:if>
        <xsl:if test="Dataset_Progress">
          <tr>
            <td><strong>Progress</strong></td>
            <td><xsl:value-of select="Dataset_Progress" /></td>
          </tr>
        </xsl:if>
        <xsl:if test="Data_Resolution/Temporal_Resolution">
          <tr>
            <td><strong>Resolution</strong></td>
            <td><xsl:value-of select="Data_Resolution/Temporal_Resolution" /></td>
          </tr>
        </xsl:if>
      </table>
    </blockquote>
    <br/>

<!-- Spatial Information table; using data under Spatial_Coverage and
     Data_Resolution tags -->
    <h3 style="margin-bottom: 0;"><strong>Spatial Information</strong></h3>
    <blockquote style="margin-top: 0;">
      <table width="800" border="1">
        <xsl:if test="Spatial_Coverage/Geometry/Bounding_Rectangle">
          <tr>
            <td width="200"><strong>Latitude Range</strong></td>
            <td><xsl:value-of select="Spatial_Coverage/Geometry/Bounding_Rectangle/Southernmost_Latitude" /><xsl:text> to </xsl:text> <xsl:value-of select="Spatial_Coverage/Geometry/Bounding_Rectangle/Northernmost_Latitude" /></td>
          </tr>
        </xsl:if>
        <xsl:if test="Data_Resolution/Latitude_Resolution">
          <tr>
            <td><strong>Latitude Resolution</strong></td>
            <td><xsl:value-of select="Data_Resolution/Latitude_Resolution" /></td>
          </tr>
        </xsl:if>
        <xsl:if test="Spatial_Coverage/Geometry/Bounding_Rectangle">
          <tr>
            <td><strong>Longitude Range</strong></td>
            <td><xsl:value-of select="Spatial_Coverage/Geometry/Bounding_Rectangle/Westernmost_Longitude" /><xsl:text> to </xsl:text><xsl:value-of select="Spatial_Coverage/Geometry/Bounding_Rectangle/Easternmost_Longitude" /></td>
          </tr>
        </xsl:if>
        <xsl:if test="Data_Resolution/Longitude_Resolution">
          <tr>
            <td><strong>Longitude Resolution</strong></td>
            <td><xsl:value-of select="Data_Resolution/Longitude_Resolution" /></td>
          </tr>
        </xsl:if>
        <xsl:if test="Data_Resolution/Horizontal_Resolution_Range">
          <tr>
            <td><strong>Horizontal Resolution</strong></td>
            <td><xsl:value-of select="Data_Resolution/Horizontal_Resolution_Range" /></td>
          </tr>
        </xsl:if>
        <xsl:if test="Data_Resolution/Vertical_Resolution">
          <tr>
            <td><strong>Vertical Resolution</strong></td>
            <td><xsl:value-of select="Data_Resolution/Vertical_Resolution" /></td>
          </tr>
        </xsl:if>
        <xsl:if test="Spatial_Coverage/Minimum_Altitude">
          <tr>
            <td><strong>Minimum Altitude</strong></td>
            <td><xsl:value-of select="Spatial_Coverage/Minimum_Altitude" /></td>
          </tr>
        </xsl:if>
        <xsl:if test="Spatial_Coverage/Maximum_Altitude">
          <tr>
            <td><strong>Maximum Altitude</strong></td>
            <td><xsl:value-of select="Spatial_Coverage/Maximum_Altitude" /></td>
          </tr>
        </xsl:if>
        <xsl:if test="Spatial_Coverage/Minimum_Depth">
          <tr>
            <td><strong>Minimum Depth</strong></td>
            <td><xsl:value-of select="Spatial_Coverage/Minimum_Depth" /></td>
          </tr>
        </xsl:if>
        <xsl:if test="Spatial_Coverage/Maximum_Depth">
          <tr>
            <td><strong>Maximum Depth</strong></td>
            <td><xsl:value-of select="Spatial_Coverage/Maximum_Depth" /></td>
          </tr>
        </xsl:if>
        <xsl:if test="Location/Location_Category">
          <tr>
            <td><strong>Location Category</strong></td>
            <td><xsl:value-of select="Location/Location_Category" /></td>
          </tr>
        </xsl:if>
        <xsl:if test="Location/Location_Type">
          <tr>
            <td><strong>Location Type</strong></td>
            <td><xsl:value-of select="Location/Location_Type" /></td>
          </tr>
        </xsl:if>
      </table>
    </blockquote>
    <br/>

<!-- Project table -->
    <h3 style="margin-bottom: 0;"><strong>Project</strong></h3>
    <blockquote style="margin-top: 0;">
      <table width="800" border="1">
        <xsl:for-each select="Project">
          <tr>
            <td><strong>Short Name</strong></td>
            <td><xsl:value-of select="Short_Name" /></td>
          </tr>
          <tr>
            <td><strong>Long Name</strong></td>
            <td><xsl:value-of select="Long_Name" /></td>
          </tr>
        </xsl:for-each>
        <xsl:if test="Quality">
          <tr>
            <td><strong>Quality</strong></td>
            <td>
              <xsl:call-template name="CopyWithLineBreaks">
                <xsl:with-param name="string" select="Quality" />
              </xsl:call-template>
            </td>
          </tr>
        </xsl:if>
        <xsl:if test="Access_Constraints">
          <tr>
            <td><strong>Access Constraints</strong></td>
            <td>
              <xsl:call-template name="CopyWithLineBreaks">
                <xsl:with-param name="string" select="Access_Constraints" />
              </xsl:call-template>
            </td>
          </tr>
        </xsl:if>
        <xsl:if test="Use_Constraints">
          <tr>
            <td><strong>Use Constraints</strong></td>
            <td>
              <xsl:call-template name="CopyWithLineBreaks">
                <xsl:with-param name="string" select="Use_Constraints" />
              </xsl:call-template>
            </td>
          </tr>
        </xsl:if>
      </table>
    </blockquote>
    <br/>

<!-- DataCenter table -->
    <h3 style="margin-bottom: 0;"><strong>Data Center</strong></h3>
    <blockquote style="margin-top: 0;">
      <table width="800" border="1">
        <xsl:for-each select="Organization">
          <tr>
            <td width="120"><strong>Short Name</strong></td>
            <td colspan="7"><xsl:value-of select="Organization_Name/Short_Name" /></td>
          </tr>
          <tr>
            <td><strong>Long Name</strong></td>
            <td colspan="7"><xsl:value-of select="Organization_Name/Long_Name" /></td>
          </tr>
          <tr>
            <td><strong>URL</strong></td>
            <td colspan="7">
              <xsl:element name="a"><xsl:attribute name="href"><xsl:value-of select="Organization_URL" /></xsl:attribute><xsl:value-of select="Organization_URL" /></xsl:element>
            </td>
          </tr>
          <tr>
            <td rowspan="6"><strong>Personnel</strong></td>
            <td>Role</td><td><xsl:value-of select="Personnel/Role" /></td>
          </tr>
          <tr>
            <td>Name</td>
            <td>
              <xsl:if test="Personnel/Contact_Person/First_Name">
                <xsl:value-of select="Personnel/Contact_Person/First_Name" />
                <xsl:text> </xsl:text>
              </xsl:if>
              <xsl:if test="Personnel/Contact_Person/Middle_Name">
                <xsl:value-of select="Personnel/Middle_Name" />
                <xsl:text> </xsl:text>
              </xsl:if>
              <xsl:value-of select="Personnel/Contact_Person/Last_Name" />
            </td>
          </tr>
          <tr>
            <td>Email</td>
            <xsl:if test="Personnel/Contact_Person/Email">
              <td>
                <xsl:element name="a"><xsl:attribute name="href"><xsl:text>mailto:</xsl:text><xsl:value-of select="Personnel/Contact_Person/Email" /></xsl:attribute><xsl:value-of select="Personnel/Contact_Person/Email" /></xsl:element>
              </td>
            </xsl:if>
          </tr>
          <tr>
            <td>Phone</td>
            <td>
              <xsl:for-each select="Personnel/Contact_Person/Phone">
                <xsl:if test="Type = 'Telephone'">
                  <xsl:value-of select="Number" />
                </xsl:if>
              </xsl:for-each>
            </td>
          </tr>
          <tr>
            <td>Fax</td>
            <td>
              <xsl:for-each select="Personnel/Contact_Person/Phone">
                <xsl:if test="Type = 'Fax'">
                  <xsl:value-of select="Number" />
                </xsl:if>
              </xsl:for-each>
            </td>
          </tr>
          <tr>
            <td>Address</td><td>
            <xsl:if test="Personnel/Contact_Person/Address">
              <xsl:for-each select="Personnel/Contact_Person/Address/Street_Address">
                <xsl:value-of select="current()" /><xsl:text>, </xsl:text>
              </xsl:for-each>
              <xsl:value-of select="Personnel/Contact_Person/Address/City" /><xsl:text>, </xsl:text>
              <xsl:value-of select="Personnel/Contact_Person/Address/State_Province" /><xsl:text>, </xsl:text>
              <xsl:value-of select="Personnel/Contact_Person/Address/Postal_Code" /><xsl:text>, </xsl:text>
              <xsl:value-of select="Personnel/Contact_Person/Address/Country" />
            </xsl:if></td>
          </tr>
        </xsl:for-each>
      </table>
    </blockquote>
    <br/>

<!-- Distribution table; optional depending on existence of Distribution tag -->
    <xsl:if test="Distribution">
      <h3 style="margin-bottom: 0;"><strong>Distribution</strong></h3>
      <blockquote style="margin-top: 0;">
        <table width="800" border="1">
          <tr>
            <td width="100"><strong>Media</strong></td>
            <td><xsl:value-of select="Distribution/Distribution_Media" /></td>
          </tr>
          <tr>
            <td><strong>Size</strong></td>
            <td><xsl:value-of select="Distribution/Distribution_Size" /></td>
          </tr>
          <tr>
            <td><strong>Format</strong></td>
            <td><xsl:value-of select="Distribution/Distribution_Format" /></td>
          </tr>
          <tr>
            <td><strong>Fees</strong></td>
            <td><xsl:value-of select="Distribution/Fees" /></td>
          </tr>
        </table>
      </blockquote>
    </xsl:if>
    <br/>

<!-- Reference table; optional depending on existence of Reference tag -->
    <xsl:if test="Reference/Citation">
      <h3 style="margin-bottom: 0;"><strong>Reference</strong></h3>
      <blockquote style="margin-top: 0;">
        <table width="800" border="1">
          <tr>
            <td>
              <xsl:for-each select="Reference/Citation">
                <xsl:call-template name="CopyWithLineBreaks">
                  <xsl:with-param name="string" select="." />
                </xsl:call-template><br/>
              </xsl:for-each>
            </td>
          </tr>
        </table>
      </blockquote>
    </xsl:if>
    <br/>

<!-- Summary table -->
    <h3 style="margin-bottom: 0;"><strong>Summary</strong></h3>
    <blockquote style="margin-top: 0;">
      <table width="800" border="1">
        <tr>
          <td>
            <xsl:call-template name="CopyWithLineBreaks">
              <xsl:with-param name="string" select="Summary" />
            </xsl:call-template>
          </td>
        </tr>
      </table>
    </blockquote>
    <br/>

<!-- Multimedia table; optional depending on existence of Multimedia_Sample tag -->
    <xsl:if test="Multimedia_Sample">
      <h3 style="margin-bottom: 0;"><strong>Multimedia Sample</strong></h3>
      <blockquote style="margin-top: 0;">
        <table width="800" border="1">
          <tr>
            <td><strong>File</strong></td>
            <td><xsl:value-of select="Multimedia_Sample/File" /></td>
          </tr>
          <tr>
            <td><strong>URL</strong></td>
            <td>
              <xsl:element name="a"><xsl:attribute name="href"><xsl:value-of select="Multimedia_Sample/URL" /></xsl:attribute><xsl:value-of select="Multimedia_Sample/URL" /></xsl:element>
            </td>
          </tr>
          <tr>
            <td><strong>Format</strong></td>
            <td><xsl:value-of select="Multimedia_Sample/Format" /></td>
          </tr>
          <tr>
            <td><strong>Caption</strong></td>
            <td><xsl:value-of select="Multimedia_Sample/Caption" /></td>
          </tr>
          <tr>
            <td><strong>Description</strong></td>
            <td><xsl:value-of select="Multimedia_Sample/Description" /></td>
          </tr>
        </table>
      </blockquote>
    </xsl:if>
    <br/>

<!-- Related URL table -->
    <h3 style="margin-bottom: 0;"><strong>Related URL</strong></h3>
    <blockquote style="margin-top: 0;">
      <xsl:for-each select="Related_URL">
        <table width="800" border="1">
          <tr>
            <td width="160"><strong>URL Content Type</strong></td>
            <td><xsl:value-of select="URL_Content_Type/Type" /></td>
          </tr>
          <xsl:if test="URL_Content_Type/Subtype">
            <tr>
              <td><strong>URL Content Sub Type</strong></td>
              <td><xsl:value-of select="URL_Content_Type/Subtype" /></td>
            </tr>
          </xsl:if>
          <xsl:for-each select="URL">
            <tr>
              <td><strong>URL</strong></td>
              <td>
                <xsl:element name="a"><xsl:attribute name="href"><xsl:value-of select="." /></xsl:attribute><xsl:value-of select="." /></xsl:element>
              </td>
            </tr>
          </xsl:for-each>
          <tr>
            <td><strong>Description</strong></td>
            <td><xsl:value-of select="Description" /></td>
          </tr>
        </table>
        <br/>
      </xsl:for-each>
    </blockquote>

<!-- Other table collecting miscellaneous information -->
    <h3 style="margin-bottom: 0;"><strong>Other</strong></h3>
    <blockquote style="margin-top: 0;">
      <table width="800" border="1">
        <xsl:if test="IDN_Node/Short_Name">
          <tr>
            <td><strong>IDN_Node Short Name</strong></td>
            <td><xsl:value-of select="IDN_Node/Short_Name" /></td>
          </tr>
        </xsl:if>
        <xsl:if test="IDN_Node/Long_Name">
          <tr>
            <td><strong>IDN_Node Long Name</strong></td>
            <td><xsl:value-of select="IDN_Node/Long_Name" /></td>
          </tr>
        </xsl:if>
        <xsl:if test="Originating_Metadata_Node">
          <tr>
            <td><strong>Originating Metadata Node</strong></td>
            <td><xsl:value-of select="Originating_Metadata_Node" /></td>
          </tr>
        </xsl:if>
        <xsl:if test="Metadata_Name">
          <tr>
            <td><strong>Metadata Name</strong></td>
            <td><xsl:value-of select="Metadata_Name" /></td>
          </tr>
        </xsl:if>
        <xsl:if test="Metadata_Version">
          <tr>
            <td><strong>Metadata Version</strong></td>
            <td><xsl:value-of select="Metadata_Version" /></td>
          </tr>
        </xsl:if>
        <xsl:if test="DIF_Creation_Date">
          <tr>
            <td><strong>DIF Creation Date</strong></td>
            <td><xsl:value-of select="DIF_Creation_Date" /></td>
          </tr>
        </xsl:if>
        <xsl:if test="Last_DIF_Revision_Date">
          <tr>
            <td><strong>Last DIF Revision Date</strong></td>
            <td><xsl:value-of select="Last_DIF_Revision_Date" /></td>
          </tr>
        </xsl:if>
        <xsl:if test="DIF_Revision_History">
          <tr>
            <td><strong>DIF Rivision History</strong></td>
            <td><xsl:value-of select="DIF_Revision_History" /></td>
          </tr>
        </xsl:if>
        <xsl:if test="Future_DIF_Review_Date">
          <tr>
            <td><strong>Future DIF Review Date</strong></td>
            <td><xsl:value-of select="Future_DIF_Review_Date" /></td>
          </tr>
        </xsl:if>
      </table>
    </blockquote>      
    <br/>
    
  </xsl:template>
  
<!-- named template "CopyWithLineBreaks"; calls named template "lf2br" -->
  <xsl:template name="CopyWithLineBreaks">
    <xsl:param name="string"/>
      <xsl:variable name="Result">
	<xsl:call-template name="lf2br">
  	  <xsl:with-param name="StringToTransform" select="$string"/>
	</xsl:call-template>
      </xsl:variable>
    <xsl:copy-of select="$Result"/>
  </xsl:template>

<!-- named template "lf2br".  Checks for and maintain linefeeds and paragraphs,
     and provide hyperlinks in text starts with 'http' or 'ftp'.  The logic is:

     if found linebreak at end of the line
       if found 'http' in the line
         if found doubleQuote in the line after 'http'
           make hyperlink with 'http' and before doubleQuote;
         else if found ')'
           make hyperlink with 'http' and before ')';
         else if found space after period '.'
           make hyperlink with 'http' and before period '.';
         else if nextLine has more stuff other than a linefeed and spaces
           make hyperlink with 'http' and nextLine;
         else
           make hyperlink for currentLine with and after 'http';
         end if;
       else if found 'ftp://' in the line
           (repeat the same logic as for 'http' block);
       else
           maintain line and linebreak;
           if nextLine is not hyperlinked
              feed stuff after linebreak to recursive self;
           else
              feed stuff after linebreak of nextLine to recursive self;
           end if;
       end if;
     else
       maintain line;
     end if;

-->

  <xsl:template name="lf2br">
    <xsl:param name="StringToTransform"/>
    <xsl:choose>
      <xsl:when test="contains($StringToTransform,'&#xA;')">
        <xsl:choose>
          <xsl:when test="contains(substring-before($StringToTransform, '&#xA;'),'http')">
            <xsl:value-of select="substring-before(substring-before($StringToTransform, '&#xA;'),'http')" />
            <xsl:choose>
              <xsl:when test="contains(substring-after(substring-before($StringToTransform, '&#xA;'), 'http'),'&quot;')">
                <xsl:element name="a"><xsl:attribute name="href"><xsl:text>http</xsl:text><xsl:value-of select="substring-before(substring-after(substring-before($StringToTransform, '&#xA;'), 'http'), '&quot;')" /></xsl:attribute><xsl:text>http</xsl:text><xsl:value-of select="substring-before(substring-after(substring-before($StringToTransform, '&#xA;'),'http'), '&quot;')" /></xsl:element>
                <xsl:text>&quot;</xsl:text>
                <xsl:value-of select="substring-after(substring-after(substring-before($StringToTransform, '&#xA;'),'http'),  '&quot;')" />
              </xsl:when>
              <xsl:otherwise>
                <xsl:choose>
                  <xsl:when test="contains(substring-after(substring-before($StringToTransform, '&#xA;'), 'http'),')')">
                    <xsl:element name="a"><xsl:attribute name="href"><xsl:text>http</xsl:text><xsl:value-of select="substring-before(substring-after(substring-before($StringToTransform, '&#xA;'), 'http'), ')')" /></xsl:attribute><xsl:text>http</xsl:text><xsl:value-of select="substring-before(substring-after(substring-before($StringToTransform, '&#xA;'),'http'), ')')" /></xsl:element>
                    <xsl:text>)</xsl:text>
                    <xsl:value-of select="substring-after(substring-after(substring-before($StringToTransform, '&#xA;'),'http'),  ')')" />
                  </xsl:when>
                  <xsl:otherwise>
                    <xsl:choose>
                      <xsl:when test="contains(substring-after(substring-before($StringToTransform, '&#xA;'), 'http'),'.&#x20;')">
                        <xsl:element name="a"><xsl:attribute name="href"><xsl:text>http</xsl:text><xsl:value-of select="substring-before(substring-after(substring-before($StringToTransform, '&#xA;'), 'http'), '.&#x20;')" /></xsl:attribute><xsl:text>http</xsl:text><xsl:value-of select="substring-before(substring-after(substring-before($StringToTransform, '&#xA;'),'http'), '.&#x20;')" /></xsl:element>
                        <xsl:text>.&#x20;</xsl:text>
                        <xsl:value-of select="substring-after(substring-after(substring-before($StringToTransform, '&#xA;'),'http'),  '.&#x20;')" />
                      </xsl:when>
                      <xsl:otherwise>
                        <xsl:choose>
		          <xsl:when test="normalize-space(substring-before(substring-after($StringToTransform, '&#xA;'), '&#xA;'))">
                            <xsl:element name="a"><xsl:attribute name="href"><xsl:text>http</xsl:text><xsl:value-of select="substring-after(substring-before($StringToTransform, '&#xA;'), 'http')" /><xsl:value-of select="normalize-space(substring-before(substring-after($StringToTransform, '&#xA;'), '&#xA;'))" /></xsl:attribute><xsl:text>http</xsl:text><xsl:value-of select="substring-after(substring-before($StringToTransform, '&#xA;'),'http')" /><xsl:value-of select="substring-before(substring-after($StringToTransform, '&#xA;'), '&#xA;')" /></xsl:element>
                          </xsl:when>
                          <xsl:otherwise>
                            <xsl:element name="a"><xsl:attribute name="href"><xsl:text>http</xsl:text><xsl:value-of select="substring-after(substring-before($StringToTransform, '&#xA;'), 'http')" /></xsl:attribute><xsl:text>http</xsl:text><xsl:value-of select="substring-after(substring-before($StringToTransform, '&#xA;'),'http')" /></xsl:element>
                          </xsl:otherwise>
                        </xsl:choose>
                      </xsl:otherwise>
                    </xsl:choose>
                  </xsl:otherwise>
                </xsl:choose>
              </xsl:otherwise>
            </xsl:choose>
          </xsl:when>
          <xsl:otherwise>
            <xsl:choose>
              <xsl:when test="contains(substring-before($StringToTransform, '&#xA;'),'ftp://')">
                <xsl:value-of select="substring-before(substring-before($StringToTransform, '&#xA;'),'ftp://')" />
                <xsl:choose>
                  <xsl:when test="contains(substring-after(substring-before($StringToTransform, '&#xA;'), 'ftp://'),'&quot;')">
                    <xsl:element name="a"><xsl:attribute name="href"><xsl:text>ftp://</xsl:text><xsl:value-of select="substring-before(substring-after(substring-before($StringToTransform, '&#xA;'), 'ftp://'), '&quot;')" /></xsl:attribute><xsl:text>ftp://</xsl:text><xsl:value-of select="substring-before(substring-after(substring-before($StringToTransform, '&#xA;'),'ftp://'), '&quot;')" /></xsl:element>
                    <xsl:text>&quot;</xsl:text>
                    <xsl:value-of select="substring-after(substring-after(substring-before($StringToTransform, '&#xA;'),'ftp://'),  '&quot;')" />
                  </xsl:when>
                  <xsl:otherwise>
                    <xsl:choose>
                      <xsl:when test="contains(substring-after(substring-before($StringToTransform, '&#xA;'), 'ftp://'),')')">
                        <xsl:element name="a"><xsl:attribute name="href"><xsl:text>ftp://</xsl:text><xsl:value-of select="substring-before(substring-after(substring-before($StringToTransform, '&#xA;'), 'ftp://'), ')')" /></xsl:attribute><xsl:text>ftp://</xsl:text><xsl:value-of select="substring-before(substring-after(substring-before($StringToTransform, '&#xA;'),'ftp://'), ')')" /></xsl:element>
                        <xsl:text>)</xsl:text>
                        <xsl:value-of select="substring-after(substring-after(substring-before($StringToTransform, '&#xA;'),'ftp://'),  ')')" />
                      </xsl:when>
                      <xsl:otherwise>
                        <xsl:choose>
                          <xsl:when test="contains(substring-after(substring-before($StringToTransform, '&#xA;'), 'ftp://'),'.&#x20;')">
                            <xsl:element name="a"><xsl:attribute name="href"><xsl:text>ftp://</xsl:text><xsl:value-of select="substring-before(substring-after(substring-before($StringToTransform, '&#xA;'), 'ftp://'), '.&#x20;')" /></xsl:attribute><xsl:text>ftp://</xsl:text><xsl:value-of select="substring-before(substring-after(substring-before($StringToTransform, '&#xA;'),'ftp://'), '.&#x20;')" /></xsl:element>
                            <xsl:text>.&#x20;</xsl:text>
                            <xsl:value-of select="substring-after(substring-after(substring-before($StringToTransform, '&#xA;'),'ftp://'),  '.&#x20;')" />
                          </xsl:when>
                          <xsl:otherwise>
                            <xsl:choose>
                              <xsl:when test="normalize-space(substring-before(substring-after($StringToTransform, '&#xA;'), '&#xA;'))">
                                <xsl:element name="a"><xsl:attribute name="href"><xsl:text>ftp://</xsl:text><xsl:value-of select="substring-after(substring-before($StringToTransform, '&#xA;'), 'ftp://')" /><xsl:value-of select="normalize-space(substring-before(substring-after($StringToTransform, '&#xA;'), '&#xA;'))" /></xsl:attribute><xsl:text>ftp://</xsl:text><xsl:value-of select="substring-after(substring-before($StringToTransform, '&#xA;'),'ftp://')" /><xsl:value-of select="substring-before(substring-after($StringToTransform, '&#xA;'), '&#xA;')" /></xsl:element>
                              </xsl:when>
                              <xsl:otherwise>
                                <xsl:element name="a"><xsl:attribute name="href"><xsl:text>ftp://</xsl:text><xsl:value-of select="substring-after(substring-before($StringToTransform, '&#xA;'), 'ftp://')" /></xsl:attribute><xsl:text>ftp://</xsl:text><xsl:value-of select="substring-after(substring-before($StringToTransform, '&#xA;'),'ftp://')" /></xsl:element>
                              </xsl:otherwise>
                            </xsl:choose>
                          </xsl:otherwise>
                        </xsl:choose>
                      </xsl:otherwise>
                    </xsl:choose>
                  </xsl:otherwise>
                </xsl:choose>
              </xsl:when>
              <xsl:otherwise>
                <xsl:value-of select="substring-before($StringToTransform,'&#xA;')"/>
              </xsl:otherwise>
            </xsl:choose>
          </xsl:otherwise>
        </xsl:choose>
        <br/>
        <xsl:choose>
          <xsl:when test="contains(substring-before($StringToTransform, '&#xA;'), 'http')">
            <xsl:choose>
              <xsl:when test="contains(substring-after(substring-before($StringToTransform, '&#xA;'), 'http'), '&quot;')">
                <xsl:call-template name="lf2br">
                  <xsl:with-param name="StringToTransform">
                    <xsl:value-of select="substring-after($StringToTransform,'&#xA;')"/>
                  </xsl:with-param>
                </xsl:call-template>
              </xsl:when>
              <xsl:otherwise>
                <xsl:choose>
                  <xsl:when test="contains(substring-after(substring-before($StringToTransform, '&#xA;'), 'http'), ')')"> 
                    <xsl:call-template name="lf2br">
                      <xsl:with-param name="StringToTransform">
                        <xsl:value-of select="substring-after($StringToTransform,'&#xA;')"/>
                      </xsl:with-param>
                    </xsl:call-template> 
                  </xsl:when>
                  <xsl:otherwise>
                    <xsl:choose>
                      <xsl:when test="contains(substring-after(substring-before($StringToTransform, '&#xA;'), 'http'), '.&#x20;')">
                        <xsl:call-template name="lf2br">
                          <xsl:with-param name="StringToTransform">
                            <xsl:value-of select="substring-after($StringToTransform,'&#xA;')"/>
                          </xsl:with-param>
                        </xsl:call-template>
                      </xsl:when>
                      <xsl:otherwise>
                        <xsl:choose>
                          <xsl:when test="normalize-space(substring-before(substring-after($StringToTransform, '&#xA;'), '&#xA;'))">
                            <xsl:call-template name="lf2br">
                              <xsl:with-param name="StringToTransform">
                                <xsl:value-of select="substring-after(substring-after($StringToTransform,'&#xA;'), '&#xA;')"/>
                              </xsl:with-param>
                            </xsl:call-template>
                          </xsl:when>
                          <xsl:otherwise>
                            <xsl:call-template name="lf2br">
                              <xsl:with-param name="StringToTransform">
                                <xsl:value-of select="substring-after($StringToTransform,'&#xA;')"/>
                              </xsl:with-param>
                            </xsl:call-template>
                          </xsl:otherwise>
                        </xsl:choose>
                      </xsl:otherwise>
                    </xsl:choose>
                  </xsl:otherwise>
                </xsl:choose>
              </xsl:otherwise>
            </xsl:choose>
          </xsl:when>
          <xsl:otherwise>
            <xsl:choose>
              <xsl:when test="contains(substring-before($StringToTransform, '&#xA;'), 'ftp://')">
                <xsl:choose>
                  <xsl:when test="contains(substring-after(substring-before($StringToTransform, '&#xA;'), 'ftp://'), '&quot;')">
                    <xsl:call-template name="lf2br">
                      <xsl:with-param name="StringToTransform">
                        <xsl:value-of select="substring-after($StringToTransform,'&#xA;')"/>
                      </xsl:with-param>
                    </xsl:call-template>
                  </xsl:when>
                  <xsl:otherwise>
                    <xsl:choose>
                      <xsl:when test="contains(substring-after(substring-before($StringToTransform, '&#xA;'), 'ftp://'), ')')">
                        <xsl:call-template name="lf2br">
                          <xsl:with-param name="StringToTransform">
                            <xsl:value-of select="substring-after($StringToTransform,'&#xA;')"/>
                          </xsl:with-param>
                        </xsl:call-template>
                      </xsl:when>
                      <xsl:otherwise>
                        <xsl:choose>
                          <xsl:when test="contains(substring-after(substring-before($StringToTransform, '&#xA;'), 'ftp://'), '.&#x20;')">
                            <xsl:call-template name="lf2br">
                              <xsl:with-param name="StringToTransform">
                                <xsl:value-of select="substring-after($StringToTransform,'&#xA;')"/>
                              </xsl:with-param>
                            </xsl:call-template>
                          </xsl:when>
                          <xsl:otherwise>
                            <xsl:choose>
                              <xsl:when test="normalize-space(substring-before(substring-after($StringToTransform, '&#xA;'), '&#xA;'))">
                                <xsl:call-template name="lf2br">
                                  <xsl:with-param name="StringToTransform">
                                    <xsl:value-of select="substring-after(substring-after($StringToTransform,'&#xA;'), '&#xA;')"/>
                                  </xsl:with-param>
                                </xsl:call-template>
                              </xsl:when>
                              <xsl:otherwise>
                                <xsl:call-template name="lf2br">
                                  <xsl:with-param name="StringToTransform">
                                    <xsl:value-of select="substring-after($StringToTransform,'&#xA;')"/>
                                  </xsl:with-param>
                                </xsl:call-template>
                              </xsl:otherwise>
                            </xsl:choose>
                          </xsl:otherwise>
                        </xsl:choose>
                      </xsl:otherwise>
                    </xsl:choose>
                  </xsl:otherwise>
                </xsl:choose>
              </xsl:when>
              <xsl:otherwise>
                <xsl:call-template name="lf2br">
                  <xsl:with-param name="StringToTransform">
                    <xsl:value-of select="substring-after($StringToTransform,'&#xA;')"/>
                  </xsl:with-param>
                </xsl:call-template>
              </xsl:otherwise>
            </xsl:choose>
          </xsl:otherwise>
        </xsl:choose>
      </xsl:when>
      <xsl:otherwise>
        <xsl:value-of select="$StringToTransform"/>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template> 

<!-- named template 'CopyWithHyperLink'; calls named template 'showlink' -->
  <xsl:template name="CopyWithHyperLink">
    <xsl:param name="link"/>
      <xsl:variable name="Address">
        <xsl:call-template name="showlink">
          <xsl:with-param name="LinkToCopy" select="$link"/>
        </xsl:call-template>
      </xsl:variable>
    <xsl:copy-of select="$Address"/>
  </xsl:template>

<!-- named template 'showlink'; makes hyperlink in text without linebreak.  The Logic is:

  if found in line 'http'
    if found in line doubleQuote after 'http'
      make hyperlink with 'http' before doubleQuote;
    else if found in line ')' after 'http'
      make hyperlink with 'http' before ')';
    else if found space after period '.' in line after 'http'
      make hyperlink with 'http' before period;
    else
      make hyperlink with and after 'http' in line;
    end if;
  else if found in line 'ftp://'
    (repeat the same logic as for 'http');
  else
    maintain text without hyperlink;
  end if;

-->
  <xsl:template name="showlink">
    <xsl:param name="LinkToCopy"/>
    <xsl:choose>
      <xsl:when test="contains($LinkToCopy,'http')">
        <xsl:value-of select="substring-before($LinkToCopy, 'http')" />
        <xsl:choose>
          <xsl:when test="contains(substring-after($LinkToCopy, 'http'), '&quot;')">
            <xsl:element name="a"><xsl:attribute name="href"><xsl:text>http</xsl:text><xsl:value-of select="substring-before(substring-after($LinkToCopy, 'http'), '&quot;')" /></xsl:attribute><xsl:text>http</xsl:text><xsl:value-of select="substring-before(substring-after($LinkToCopy,'http'), '&quot;')" /></xsl:element>
            <xsl:value-of select="substring-after(substring-after($LinkToCopy, 'http'), '&quot;')" />
          </xsl:when>
          <xsl:otherwise>
            <xsl:choose>
              <xsl:when test="contains(substring-after($LinkToCopy, 'http'), ')')">
                <xsl:element name="a"><xsl:attribute name="href"><xsl:text>http</xsl:text><xsl:value-of select="substring-before(substring-after($LinkToCopy, 'http'), ')')" /></xsl:attribute><xsl:text>http</xsl:text><xsl:value-of select="substring-before(substring-after($LinkToCopy,'http'), ')')" /></xsl:element>
                <xsl:value-of select="substring-after(substring-after($LinkToCopy, 'http'), ')')" />
              </xsl:when>
              <xsl:otherwise>
                <xsl:choose>
                  <xsl:when test="contains(substring-after($LinkToCopy, 'http'), '.&#x20;')">
                    <xsl:element name="a"><xsl:attribute name="href"><xsl:text>http</xsl:text><xsl:value-of select="substring-before(substring-after($LinkToCopy, 'http'), '.&#x20;')" /></xsl:attribute><xsl:text>http</xsl:text><xsl:value-of select="substring-before(substring-after($LinkToCopy,'http'), '.&#x20;')" /></xsl:element>
                    <xsl:value-of select="substring-after(substring-after($LinkToCopy, 'http'), '.&#x20;')" />
                  </xsl:when>
                  <xsl:otherwise>
                    <xsl:element name="a"><xsl:attribute name="href"><xsl:text>http</xsl:text><xsl:value-of select="substring-after($LinkToCopy, 'http')" /></xsl:attribute><xsl:text>http</xsl:text><xsl:value-of select="substring-after($LinkToCopy,'http')" /></xsl:element>
                  </xsl:otherwise>
                </xsl:choose>
              </xsl:otherwise>
            </xsl:choose>
          </xsl:otherwise> 
        </xsl:choose>
      </xsl:when>
      <xsl:otherwise>
        <xsl:choose>
          <xsl:when test="contains($LinkToCopy,'ftp://')">
            <xsl:value-of select="substring-before($LinkToCopy, 'ftp://')" />
            <xsl:choose>
              <xsl:when test="contains(substring-after($LinkToCopy, 'ftp://'), '&quot;')">
                <xsl:element name="a"><xsl:attribute name="href"><xsl:text>ftp://</xsl:text><xsl:value-of select="substring-before(substring-after($LinkToCopy, 'ftp://'), '&quot;')" /></xsl:attribute><xsl:text>ftp://</xsl:text><xsl:value-of select="substring-before(substring-after($LinkToCopy,'ftp://'), '&quot;')" /></xsl:element>
                <xsl:value-of select="substring-after(substring-after($LinkToCopy, 'ftp://'), '&quot;')" />
              </xsl:when>
              <xsl:otherwise>
                <xsl:choose>
                  <xsl:when test="contains(substring-after($LinkToCopy, 'ftp://'), ')')">
                    <xsl:element name="a"><xsl:attribute name="href"><xsl:text>ftp://</xsl:text><xsl:value-of select="substring-before(substring-after($LinkToCopy, 'ftp://'), ')')" /></xsl:attribute><xsl:text>ftp://</xsl:text><xsl:value-of select="substring-before(substring-after($LinkToCopy,'ftp://'), ')')" /></xsl:element>
                    <xsl:value-of select="substring-after(substring-after($LinkToCopy, 'ftp://'), ')')" />
                  </xsl:when>
                  <xsl:otherwise>
                    <xsl:choose>
                      <xsl:when test="contains(substring-after($LinkToCopy, 'ftp://'), '.&#x20;')">
                        <xsl:element name="a"><xsl:attribute name="href"><xsl:text>ftp://</xsl:text><xsl:value-of select="substring-before(substring-after($LinkToCopy, 'ftp://'), '.&#x20;')" /></xsl:attribute><xsl:text>ftp://</xsl:text><xsl:value-of select="substring-before(substring-after($LinkToCopy,'ftp://'), '.&#x20;')" /></xsl:element>
                        <xsl:value-of select="substring-after(substring-after($LinkToCopy, 'ftp://'), '.&#x20;')" />
                      </xsl:when>
                      <xsl:otherwise>
                        <xsl:element name="a"><xsl:attribute name="href"><xsl:text>ftp://</xsl:text><xsl:value-of select="substring-after($LinkToCopy, 'ftp://')" /></xsl:attribute><xsl:text>ftp://</xsl:text><xsl:value-of select="substring-after($LinkToCopy,'ftp://')" /></xsl:element>
                      </xsl:otherwise>
                    </xsl:choose>
                  </xsl:otherwise>
                </xsl:choose>
              </xsl:otherwise>
            </xsl:choose>
          </xsl:when>
          <xsl:otherwise>
            <xsl:value-of select="$LinkToCopy" />
          </xsl:otherwise>
        </xsl:choose>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template>

</xsl:stylesheet>
