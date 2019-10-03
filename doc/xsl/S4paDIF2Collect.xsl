<?xml version="1.0" encoding="UTF-8"?>
<!-- $Id: S4paDIF2Collect.xsl,v 1.6 2008/09/30 13:56:29 glei Exp $ -->
<!-- -@@@ S4PA, Version $Name:  $ -->
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">

  <xsl:output method="xml" version="1.0" encoding="UTF-8" indent="yes" />
  <!--  Schema at "http://daac.gsfc.nasa.gov/xsd/s4pa/S4paCollection.xsd" -->

  <xsl:template match="DIF">
    <S4PACollectionMetaDataFile>
      <CollectionMetaData>
        <!-- ShortName -->
        <ShortName>
          <xsl:apply-templates select="./Entry_ID" />
          <!-- <xsl:apply-templates select="./Data_Center/Data_Set_ID" /> -->
        </ShortName>

        <!-- LongName -->
        <LongName>
          <xsl:apply-templates select="./Data_Set_Citation/Dataset_Title" />
        </LongName>

        <!-- VersionID -->
        <VersionID>
          <xsl:apply-templates select="./Data_Set_Citation/Version" />
        </VersionID>

        <!-- InsertTime -->
        <InsertTime>
          <xsl:apply-templates select="//DIF_Creation_Date" />
        </InsertTime>

        <!-- LastUpdate -->
        <LastUpdate>
          <xsl:apply-templates select="./Last_DIF_Revision_Date" />
        </LastUpdate>

        <!-- ScienceKeywords -->
        <ScienceKeywords>
          <xsl:apply-templates select="Keyword" />
        </ScienceKeywords>

        <!-- DataCenter -->
        <DataCenter>
          <xsl:apply-templates select="./Data_Center/Data_Center_Name/Short_Name" />
        </DataCenter>
 
        <!-- Summary -->
        <Summary>
          <xsl:apply-templates select="./Summary" />
        </Summary>

        <!-- TemporalCoverage -->
        <xsl:apply-templates select="Temporal_Coverage" />

        <!-- SpatialCoverage -->
        <xsl:apply-templates select="Spatial_Coverage" />

        <!-- DataResolution -->
         <xsl:apply-templates select="Data_Resolution" />
 
        <!-- Platform -->
        <xsl:apply-templates select="Source_Name" />

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

  <xsl:template match="Keyword">
    <ScienceKeyword><xsl:value-of select="." /></ScienceKeyword>
  </xsl:template>

  <xsl:template match="Temporal_Coverage">
    <TemporalCoverage>
      <xsl:apply-templates select="Start_Date" />
      <xsl:apply-templates select="Stop_Date" />
    </TemporalCoverage>
  </xsl:template>

  <xsl:template match="Start_Date">
    <StartDate>
      <xsl:value-of select="." />
    </StartDate>
  </xsl:template>

  <xsl:template match="Stop_Date">
    <StopDate>
      <xsl:value-of select="." />
    </StopDate>
  </xsl:template>

  <xsl:template match="Spatial_Coverage">
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

  <xsl:template match="Source_Name">
    <Platform>
      <PlatformShortName>
        <xsl:apply-templates select="Short_Name" />
      </PlatformShortName>
      <PlatformLongName>
        <xsl:apply-templates select="Long_Name" />
      </PlatformLongName>
      <PlatformType/>
      <Instrument>
        <InstrumentShortName>
          <xsl:apply-templates select="//Sensor_Name/Short_Name" />
        </InstrumentShortName>
        <Sensor>
          <SensorShortName>
            <xsl:apply-templates select="//Sensor_Name/Short_Name" />
          </SensorShortName>
          <SensorLongName>
            <xsl:apply-templates select="//Sensor_Name/Long_Name" />
          </SensorLongName>
        </Sensor>
      </Instrument>
    </Platform>
  </xsl:template>

</xsl:stylesheet>
