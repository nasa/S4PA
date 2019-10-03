<?xml version="1.0" encoding="utf-8"?>
<!-- $Id: S4paGran2EchoBrowse10.xsl,v 1.3 2010/01/08 16:47:20 eseiler Exp $ -->
<!-- -@@@ S4PA, Version $Name:  $ -->
<xsl:stylesheet
xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">
  <xsl:output method="xml" version="1.0" encoding="UTF-8" indent="yes"/>

  <!-- This is an XSLT stylesheet that transforms S4PA granule metadata
       to ECHO Browse metadata, using the ECHO 10 schema.
    -->
  <xsl:template match="/">
    <BrowseMetaDataFile xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="http://www.echo.nasa.gov/ingest/schemas/operations/Browse.xsd">
      <BrowseImages>
        <xsl:for-each select="S4PAGranuleMetaDataFile">
          <BrowseImage>
            <ProviderBrowseId>
              <xsl:value-of select="DataGranule/BrowseFile"/>
            </ProviderBrowseId>
            <xsl:choose>
              <xsl:when test="substring(DataGranule/InsertDateTime,11,1) = ' '">
                <!-- If there is a space between date and time, e.g.
                     2007-04-12 23:37:57.000 then convert to the format
                     2007-04-12T23:37:57.000Z -->
                <InsertTime>
                  <xsl:value-of select="substring-before(DataGranule/InsertDateTime, ' ')" />
                  <xsl:text>T</xsl:text>
                  <xsl:value-of select="substring-after(DataGranule/InsertDateTime, ' ')" />
                  <xsl:text>Z</xsl:text>
                </InsertTime>
                <LastUpdate>
                  <xsl:value-of select="substring-before(DataGranule/InsertDateTime, ' ')" />
                  <xsl:text>T</xsl:text>
                  <xsl:value-of select="substring-after(DataGranule/InsertDateTime, ' ')" />
                  <xsl:text>Z</xsl:text>
                </LastUpdate>
              </xsl:when>
              <xsl:otherwise>
                <InsertTime><xsl:value-of select="DataGranule/InsertDateTime"/></InsertTime>
                <LastUpdate><xsl:value-of select="DataGranule/InsertDateTime"/></LastUpdate>
              </xsl:otherwise>
            </xsl:choose>
            <FileURL/>
            <FileSize/>
            <Description/>
          </BrowseImage>
        </xsl:for-each>
      </BrowseImages>
    </BrowseMetaDataFile>
  </xsl:template>
</xsl:stylesheet> 
