<?xml version="1.0" encoding="utf-8"?>
<!-- $Id: S4paGran2DotChart.xsl,v 1.5 2011/03/02 13:57:14 glei Exp $ -->
<!-- -@@@ S4PA, Version $Name:  $ -->
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
<xsl:output method="xml" indent="yes"/>
 <!-- This XSLT transform S4PA Granule level XML Meta-data files into DotChart ingest files. -->
  <xsl:template match="/">
    <xsl:for-each select="S4PAGranuleMetaDataFile">
    <GranuleMetaDataFile>
      <GranuleMetaDataSet>
        <Granules>
          <GranuleURMetaData>
            <CollectionMetaData>
              <xsl:copy-of select="CollectionMetaData/ShortName"/>
              <xsl:copy-of select="CollectionMetaData/VersionID"/>
              <MetaDataURL/>
            </CollectionMetaData>
            <DataGranule>
              <xsl:copy-of select="DataGranule/GranuleID"/>
              <xsl:copy-of select="DataGranule/SizeBytesDataGranule"/>
              <LocalGranuleID><xsl:value-of select="DataGranule/LocalGranuleID"/></LocalGranuleID>
              <InsertDateTime><xsl:value-of select="DataGranule/InsertDateTime"/></InsertDateTime>
              <ProductionDateTime><xsl:value-of select="DataGranule/ProductionDateTime"/></ProductionDateTime>
              <DayNightFlag><xsl:value-of select="DataGranule/DayNightFlag"/></DayNightFlag>
              <SorceTelemetryPass>
                <xsl:for-each select="PSAs/PSA">
                <xsl:if test="PSAName = 'SorceTelemetryFileIdentifier'">
                <xsl:value-of select="PSAValue"/>
                </xsl:if>
                <xsl:if test="PSAName = 'GloryTelemetryFileIdentifier'">
                <xsl:value-of select="PSAValue"/>
                </xsl:if>
                </xsl:for-each></SorceTelemetryPass>
              <Granulits/>
            </DataGranule>
            <RangeDateTime>
              <xsl:copy-of select="RangeDateTime/RangeEndingTime"/>
              <xsl:copy-of select="RangeDateTime/RangeEndingDate"/>
              <xsl:copy-of select="RangeDateTime/RangeBeginningTime"/>
              <xsl:copy-of select="RangeDateTime/RangeBeginningDate"/>
            </RangeDateTime>
          </GranuleURMetaData>
        </Granules>
      </GranuleMetaDataSet>
    </GranuleMetaDataFile>
   </xsl:for-each>
  </xsl:template>
</xsl:stylesheet>
