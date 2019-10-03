<?xml version="1.0" encoding="utf-8"?>
<!-- $Id: S4paGran2EchoBrowse.xsl,v 1.5 2007/02/26 18:21:12 hegde Exp $ -->
<!-- -@@@ S4PA, Version $Name:  $ -->
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
  <xsl:output method="xml" indent="yes"/>
  <!-- This XSLT can be used to transform S4PA Granule level XML Meta-data -->
  <!-- files into ECHO browse reference metadata ingest files. -->
  <xsl:template match="/">
    <xsl:for-each select="S4PAGranuleMetaDataFile">
      <BrowseReferenceFile>
        <BrowseCrossReference>
          <GranuleUR><xsl:value-of select="CollectionMetaData/ShortName"/>.<xsl:value-of select="CollectionMetaData/VersionID"/>:<xsl:value-of select="DataGranule/GranuleID"/></GranuleUR>
          <InsertTime><xsl:value-of select="DataGranule/InsertDateTime"/></InsertTime>
          <LastUpdate/>
          <InternalFileName><xsl:value-of select="DataGranule/BrowseFile"/></InternalFileName>
          <BrowseDescription/>
          <BrowseSize/>
        </BrowseCrossReference>
      </BrowseReferenceFile>
    </xsl:for-each>
  </xsl:template>
</xsl:stylesheet> 
