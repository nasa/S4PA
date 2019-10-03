<?xml version="1.0" encoding="utf-8"?>
<!-- $Id: S4paGran2Mirador.xsl,v 1.8 2012/06/28 17:56:23 glei Exp $ -->
<!-- -@@@ S4PA, Version $Name:  $ -->
<xsl:stylesheet
          version="1.0"
          xmlns:xsl="http://www.w3.org/1999/XSL/Transform">

<xsl:output method="xml" version="1.0" encoding="UTF-8" indent="yes"/>

 <!-- This XSLT can be used to transform S4PA Granule level XML Meta-data
      files into Mirador Format XML. -->

<xsl:template match="/S4PAGranuleMetaDataFile">
  <S4PAGranuleMetaDataFile>
    <xsl:if test="@INSTANCE">
      <xsl:attribute name="INSTANCE">
        <xsl:value-of select="@INSTANCE"/>
      </xsl:attribute>
    </xsl:if>
    <xsl:copy-of select="CollectionMetaData"/>
    <xsl:copy-of select="DataGranule"/>
    <xsl:copy-of select="RangeDateTime"/>
    <xsl:copy-of select="SpatialDomainContainer"/>
    <xsl:copy-of select="OrbitCalculatedSpatialDomain"/>
    <xsl:copy-of select="PSAs"/>
  </S4PAGranuleMetaDataFile>
</xsl:template>
</xsl:stylesheet>
