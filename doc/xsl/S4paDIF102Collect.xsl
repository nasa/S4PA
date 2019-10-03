<?xml version="1.0" encoding="UTF-8"?>
<!-- $Id: S4paDIF102Collect.xsl,v 1.1 2015/12/11 13:36:00 glei Exp $ -->
<!-- -@@@ S4PA, Version $Name:  $ -->
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">

  <xsl:output method="xml" version="1.0" encoding="UTF-8" indent="yes" />
  <!--  Schema at "http://daac.gsfc.nasa.gov/xsd/s4pa/S4paCollection.xsd" -->

  <xsl:template match="DIF">
    <S4PACollectionMetaDataFile>
      <CollectionMetaData>
        <!-- ShortName -->
        <ShortName>
          <xsl:apply-templates select="./Entry_ID/Short_Name" />
        </ShortName>

        <!-- LongName -->
        <LongName>
          <xsl:apply-templates select="./Dataset_Citation/Dataset_Title" />
        </LongName>

        <!-- VersionID -->
        <VersionID>
          <xsl:apply-templates select="./Entry_ID/Version" />
        </VersionID>

        <!-- InsertTime -->
        <InsertTime>
          <xsl:apply-templates select="./Metadata_Dates/Metadata_Creation" />
        </InsertTime>

        <!-- LastUpdate -->
        <LastUpdate>
          <xsl:apply-templates select="./Metadata_Dates/Metadata_Last_Revision" />
        </LastUpdate>

        <!-- ScienceKeywords -->
        <ScienceKeywords>
          <xsl:for-each select="Ancillary_Keyword">
            <xsl:apply-templates select="." />
          </xsl:for-each>
        </ScienceKeywords>

        <!-- DataCenter -->
        <DataCenter>
          <xsl:apply-templates select="./Organization/Organization_Name/Short_Name" />
        </DataCenter>
 
        <!-- Summary -->
        <Summary>
          <xsl:apply-templates select="./Summary" />
        </Summary>

        <!-- Temporal_Coverage -->
        <xsl:apply-templates select="Temporal_Coverage/Range_DateTime" />

        <!-- Spatial_Coverage -->
        <xsl:apply-templates select="Spatial_Coverage/Geometry/Bounding_Rectangle" />

        <!-- Data_Resolution -->
        <xsl:apply-templates select="Data_Resolution" />
 
        <!-- Platform -->
        <xsl:for-each select="Platform">
          <xsl:apply-templates select="." />
        </xsl:for-each>

        <!-- OriginatingCenter -->
        <OriginatingCenter>
          <xsl:apply-templates select="./Originating_Center" />
        </OriginatingCenter>
      </CollectionMetaData>
    </S4PACollectionMetaDataFile>
  </xsl:template>


  <xsl:template match="Entry_Title">
    <LongName><xsl:value-of select="." /></LongName>
  </xsl:template>

  <xsl:template match="Ancillary_Keyword">
    <ScienceKeyword><xsl:value-of select="." /></ScienceKeyword>
  </xsl:template>

  <xsl:template match="Temporal_Coverage/Range_DateTime">
    <TemporalCoverage>
      <xsl:apply-templates select="Beginning_Date_Time" />
      <xsl:apply-templates select="Ending_Date_Time" />
    </TemporalCoverage>
  </xsl:template>

  <xsl:template match="Beginning_Date_Time">
    <StartDate>
      <xsl:value-of select="." />
    </StartDate>
  </xsl:template>

  <xsl:template match="Ending_Date_Time">
    <StopDate>
      <xsl:value-of select="." />
    </StopDate>
  </xsl:template>

  <xsl:template match="Spatial_Coverage/Geometry/Bounding_Rectangle">
    <SpatialCoverage>
      <xsl:apply-templates select="Westernmost_Longitude" />
      <xsl:apply-templates select="Northernmost_Latitude" />
      <xsl:apply-templates select="Easternmost_Longitude" />
      <xsl:apply-templates select="Southernmost_Latitude" />
    </SpatialCoverage>
  </xsl:template>

  <xsl:template match="Westernmost_Longitude">
    <WestBoundingCoordinate>
      <xsl:value-of select="." />
    </WestBoundingCoordinate>
  </xsl:template>

  <xsl:template match="Northernmost_Latitude">
    <NorthBoundingCoordinate>
      <xsl:value-of select="." />
    </NorthBoundingCoordinate>
  </xsl:template>

  <xsl:template match="Easternmost_Longitude">
    <EastBoundingCoordinate>
      <xsl:value-of select="." />
    </EastBoundingCoordinate>
  </xsl:template>

  <xsl:template match="Southernmost_Latitude">
    <SouthBoundingCoordinate>
      <xsl:value-of select="." />
    </SouthBoundingCoordinate>
  </xsl:template>

  <xsl:template match="Data_Resolution">
    <DataResolution>
      <xsl:apply-templates select="Latitude_Resolution" />
      <xsl:apply-templates select="Longitude_Resolution" />
      <xsl:apply-templates select="Vertical_Resolution" />
      <xsl:apply-templates select="Temporal_Resolution" />
    </DataResolution>
  </xsl:template>

  <xsl:template match="Latitude_Resolution">
    <LatitudeResolution><xsl:value-of select="." /></LatitudeResolution>
  </xsl:template>

  <xsl:template match="Longitude_Resolution">
    <LongitudeResolution><xsl:value-of select="." /></LongitudeResolution>
  </xsl:template>

  <xsl:template match="Vertical_Resolution">
    <AltitudeResolution><xsl:value-of select="." /></AltitudeResolution>
    <DepthResolution><xsl:value-of select="." /></DepthResolution>
  </xsl:template>

  <xsl:template match="Temporal_Resolution">
    <TemporalResolution><xsl:value-of select="." /></TemporalResolution>
  </xsl:template>

  <xsl:template match="Platform">
    <Platform>
      <PlatformShortName>
        <xsl:apply-templates select="Short_Name" />
      </PlatformShortName>
      <PlatformLongName>
        <xsl:apply-templates select="Long_Name" />
      </PlatformLongName>
      <PlatformType>
        <xsl:apply-templates select="Type" />
      </PlatformType>
      <xsl:for-each select="./Instrument">
      <Instrument>
        <InstrumentShortName>
          <xsl:apply-templates select="Short_Name" />
        </InstrumentShortName>
        <xsl:for-each select="./Sensor">
        <Sensor>
          <SensorShortName>
            <xsl:apply-templates select="Short_Name" />
          </SensorShortName>
          <SensorLongName>
            <xsl:apply-templates select="Long_Name" />
          </SensorLongName>
        </Sensor>
        </xsl:for-each>
      </Instrument>
      </xsl:for-each>
    </Platform>
  </xsl:template>

</xsl:stylesheet>
