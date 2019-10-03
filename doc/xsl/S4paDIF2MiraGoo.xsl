<?xml version="1.0" encoding="utf-8"?>
<!-- $Id: S4paDIF2MiraGoo.xsl,v 1.8 2008/09/30 13:56:29 glei Exp $ -->
<!-- -@@@ S4PA, Version $Name:  $ -->
<xsl:stylesheet
          version="1.0"
          xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
    <xsl:output method="html" version="4.0" encoding="UTF-8" indent="yes"/>
 <!-- This XSLT can be used to transform a GCMD DIF XML
      files into an XML format Mirador's Google Appliance recognizes. -->
  <xsl:template match="/">
    <xsl:for-each select="DIF">
    <HTML>
        <HEAD>
            <!--
            Note: There can be more than one Data_Set_Citation in the DIF.
            -->
            <TITLE><xsl:value-of select="Data_Set_Citation/Dataset_Title"/></TITLE>
            <META name="params">
                <xsl:attribute name="content">
                  <xsl:for-each select="Parameters">
                    <!-- If a Parameter has both a Variable and a
                         Detailed_Variable, use the Detailed_Variable -->
                    <xsl:choose>
                      <xsl:when test="Detailed_Variable">
                        <xsl:value-of select="Detailed_Variable"/>
                      </xsl:when>
                      <xsl:otherwise>
                        <xsl:value-of select="Variable_Level_1"/>
                      </xsl:otherwise> 
                    </xsl:choose>
                    <xsl:if test = "not(position()=last())" >
                      <xsl:text>, </xsl:text>
                    </xsl:if>
                  </xsl:for-each>                    
                </xsl:attribute>
            </META> 
            <META name="platform">
                <xsl:attribute name="content">
                  <xsl:value-of select="Source_Name/Short_Name"/>
                </xsl:attribute>
            </META>
            <META name="instrument">
                <xsl:attribute name="content">
                  <xsl:value-of select="Sensor_Name/Short_Name"/>
                </xsl:attribute>
            </META>
            <META name="descurl">
                <!--
                 Note: There can be more than one Data_Set_Citation in the DIF.
                 -->
                <xsl:attribute name="content">
                  <xsl:value-of select="Data_Set_Citation/Online_Resource"/>
                </xsl:attribute>
            </META>
            <META name="resolution">
                <xsl:attribute name="content">
                  <xsl:choose>
                    <xsl:when test="(Data_Resolution/Latitude_Resolution/text()) and (Data_Resolution/Longitude_Resolution/text())">
                      <xsl:value-of select="Data_Resolution/Latitude_Resolution"/>
                      <xsl:text> x </xsl:text>
                      <xsl:value-of select="Data_Resolution/Longitude_Resolution"/>
                    </xsl:when>
                    <xsl:otherwise>
                      <xsl:text>Not Available</xsl:text>
                    </xsl:otherwise> 
                  </xsl:choose>
                </xsl:attribute>
            </META>
        </HEAD>
        <BODY>
           <P>
               <xsl:copy-of select="/"/>
           </P>
        </BODY>
    </HTML>
    </xsl:for-each>
  </xsl:template>
</xsl:stylesheet>
