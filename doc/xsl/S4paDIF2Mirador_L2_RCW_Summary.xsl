<?xml version="1.0" encoding="UTF-8"?>
<!-- $Id: S4paDIF2Mirador_L2_RCW_Summary.xsl,v 1.7 2008/09/30 13:56:29 glei Exp $ -->
<!-- -@@@ S4PA, Version: $Name:  $ -->
<xsl:stylesheet
version="1.0" 
xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
>

<xsl:output
  method="xml"
  version="1.0"
  encoding="UTF-8"
  indent="yes"
  omit-xml-declaration="yes"
/>

<!--
This is an XSLT style sheet that converts metadata in DIF XML format to
Mirador Data Product Page HTML (XHTML) format.
It is intended to replace an editable instance in a dwt template that
has the name "Summary".
-->

<xsl:template match="DIF">

 <span class="bold"><!-- TemplateBeginEditable name="ESDT Short Name" -->Data Set Short Name: <!-- TemplateEndEditable --></span> 
                                        <!-- TemplateBeginEditable name="ShortName" -->

                                        <xsl:apply-templates select="ShortName" /> <br />
                                        <!-- TemplateEndEditable --> 
										<span class="bold"><!-- TemplateBeginEditable name="ESDT Long Name" -->Data Set Long Name: <!-- TemplateEndEditable --></span> 
                                        <!-- TemplateBeginEditable name="LongName" -->
                                        <xsl:apply-templates select="Entry_Title" /> <br />
                                        <!-- TemplateEndEditable --> 
										<span class="bold"><!-- TemplateBeginEditable name="Platform" -->Platform: <!-- TemplateEndEditable --></span> 
                                        <!-- TemplateBeginEditable name="PlatformInfo" -->
                                        <xsl:choose> 
                                        <xsl:when test="Source_Name/Long_Name"> 
                                          <xsl:apply-templates select="Source_Name/Long_Name" /> <br /> 
                                        </xsl:when> 
                                        <xsl:otherwise> 
                                          <xsl:apply-templates select="Source_Name/Short_Name" /> <br /> 
                                        </xsl:otherwise> 
                                        </xsl:choose> 
                                        <!-- TemplateEndEditable --> 
										<span class="bold"><!-- TemplateBeginEditable name="Sensor" -->Sensor: <!-- TemplateEndEditable --></span> 
                                        <!-- TemplateBeginEditable name="SensorInfo" -->
                                        <xsl:apply-templates select="Sensor_Name/Long_Name" /> <br />
                                        <!-- TemplateEndEditable --> 
										<span class="bold"><!-- TemplateBeginEditable name="Resolution" -->Resolution: <!-- TemplateEndEditable --></span> 
                                        <!-- TemplateBeginEditable name="ResolutionInfo" -->
                                        <xsl:apply-templates select="Data_Resolution" /> <br />
                                        <!-- TemplateEndEditable --> 
										<span class="bold"><!-- TemplateBeginEditable name="Parameters" -->Parameters: <!-- TemplateEndEditable --></span> 
                                        <!-- TemplateBeginEditable name="ParameterInfor" -->
                                        <xsl:apply-templates select="Parameters/Variable_Level_1" /> <br />
                                        <!-- TemplateEndEditable -->
									                                               <xsl:apply-templates select="Multimedia_Sample" /> <br />

</xsl:template>

<!-- =========================== End HTML =============================== -->
<!-- ==================================================================== -->

<!-- ================== Templates start here ==================== -->

<xsl:template match="Entry_ID">
   <xsl:value-of select="." />
</xsl:template>

<xsl:template match="Entry_Title">
   <xsl:value-of select="." />
</xsl:template>

<xsl:template match="Source_Name/Long_Name">
   <xsl:value-of select="." />
   <xsl:if test = "not(position()=last())" >
      <xsl:text>, </xsl:text>
   </xsl:if> 
</xsl:template>

<xsl:template match="Source_Name/Long_Name">
   <xsl:value-of select="." />
   <xsl:if test = "not(position()=last())" >
      <xsl:text>, </xsl:text>
   </xsl:if> 
</xsl:template>

<xsl:template match="Sensor_Name/Short_Name">
   <xsl:value-of select="." />
   <xsl:if test = "not(position()=last())" >
      <xsl:text>, </xsl:text>
   </xsl:if> 
</xsl:template>

<xsl:template match="Sensor_Name/Long_Name">
   <xsl:value-of select="." />
   <xsl:if test = "not(position()=last())" >
      <xsl:text>, </xsl:text>
   </xsl:if> 
</xsl:template>

<xsl:template match="Data_Resolution">
   <xsl:value-of select="./Vertical_Resolution" />
   <xsl:if test="./Vertical_Resolution and ./Temporal_Resolution">
      <xsl:text>, </xsl:text>
   </xsl:if>
   <xsl:value-of select="./Temporal_Resolution" />
   <xsl:if test="./Temporal_Resolution and ./Temporal_Resolution_Range">
      <xsl:text>, </xsl:text>
   </xsl:if>
   <xsl:value-of select="./Temporal_Resolution_Range" />
   <xsl:if test = "not(position()=last())" >
      <xsl:text>; </xsl:text>
   </xsl:if> 
</xsl:template>

<xsl:template match="Parameters/Variable_Level_1">
   <xsl:value-of select="." />
   <xsl:if test = "not(position()=last())" >
      <xsl:text>, </xsl:text>
   </xsl:if> 
</xsl:template>

<xsl:template match="Multimedia_Sample">
  <span class="bold"><!-- TemplateBeginEditable name="SampleImage" -->Sample Image: <!-- TemplateEndEditable --></span>
  <!-- TemplateBeginEditable name="SampleImageInfor" -->
  <a><xsl:attribute name="target">_blank</xsl:attribute><xsl:attribute name="href"><xsl:value-of select="./URL" /></xsl:attribute>View full image<br/><img><xsl:attribute name="src"><xsl:value-of select="./URL" /></xsl:attribute><xsl:attribute name="title">Click to view full size image</xsl:attribute><xsl:attribute name="alt">Small multimedia sample image</xsl:attribute><xsl:attribute name="width">260</xsl:attribute></img></a>
  <!-- TemplateEndEditable -->
</xsl:template>

</xsl:stylesheet>
