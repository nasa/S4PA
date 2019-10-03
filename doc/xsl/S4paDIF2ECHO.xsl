<?xml version="1.0" encoding="UTF-8"?>
<!-- $Id: S4paDIF2ECHO.xsl,v 1.41 2010/02/16 18:34:39 eseiler Exp $ -->
<!-- -@@@ S4PA, Version $Name:  $ -->
<xsl:stylesheet 
xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0" 
xmlns:java="http://xml.apache.org/xslt/java" exclude-result-prefixes="java">

<xsl:output method="xml" version="1.0" encoding="UTF-8" indent="yes" doctype-system="http://www.echo.nasa.gov/dtd9/v9.0/ingestDTDs/ECHO90_collection.dtd" />

<!-- This is an XSLT style sheet that converts
     metadata in DIF XML format to ECHO CollectionMetadata XML format.
-->

<!-- =========== Warning: This is a work in progress =====================  -->

<!-- Author: Scott Ritz -->
<!-- Employer: SSAI, Inc -->
<!-- Created: January 2005 -->
<!-- Modified: September 2005  -->
<!-- Modified: December 2005  -->
<!-- Modified: January 2006 -->
<!-- Modified: February 2006 -->
<!-- revised: 03/16/2006 lhf, remove platform portion, add <PLATFORM_PLACEHOLDER> tag for s4pa_convert_DIF.pl -->
<!-- Modified: 06/06/2006 ejs -->
<!-- -->
<!-- Contact: ritz@gcmd.nasa.gov -->

<!-- apply templates -->

<xsl:template match="DIF">
  <CollectionMetaDataFile>

    <!-- Version of DTD Used -->
    <DTDVersion>9.0</DTDVersion>

    <!-- ID of the site that stores this metadata  -->
    <!-- Max. length of Data_Center/Data_Center_Name/Short_Name is 160 chars.
         There can be more than one Data_Center in the DIF.
         Max. length of ECHO DataCenterId is 80 chars. -->
    <DataCenterId>
      <xsl:apply-templates select="./Data_Center/Data_Center_Name/Short_Name" mode="DataCenterId" />
    </DataCenterId>

    <!-- Temporal Coverage of the Metadata Document -->
    <TemporalCoverage>
      <StartDate>
        <xsl:apply-templates select="./DIF_Creation_Date" /><xsl:text> 00:00:00</xsl:text>
      </StartDate>
      <EndDate>
        <xsl:apply-templates select="./Last_DIF_Revision_Date" /><xsl:text> 00:00:00</xsl:text>
      </EndDate>
    </TemporalCoverage>

    <!-- =========== Beginning of Collection Metadata Document ==========  -->

    <CollectionMetaDataSets>
      <Collections>
        <CollectionMetaData>

          <!-- ECHO Short_Name, the official reference name used to identify
               the contents of the data collection (required). -->
          <!-- Max. length of Entry_ID is 80 characters.
               Max. length of ECHO ShortName is 80 characters. -->
          <ShortName>
            <!-- <xsl:apply-templates select="./Data_Center/Data_Set_ID" /> -->
            <!-- (We expect ShortName to be inserted into the DIF by
                 s4pa_convert_DIF.pl, since ShortName is
                 not a field currently defined for a DIF. -->
            <xsl:apply-templates select="./ShortName" />
          </ShortName>

          <!-- ECHO VersionID, the version identifier of the data
               collection (required) -->
          <!-- Max. length of Data_Set_Citation/Version is 80 chars.
               There can be more than one Data_Set_Citation in the DIF.
               ECHO VersionID data type is N5,2 e.g. nnn.nn -->
          <VersionID>
            <xsl:apply-templates select="./Data_Set_Citation/Version" />
          </VersionID>

          <!-- No DIF Mapping, so we'll provide an alternate value -->
          <!-- The insert date/time the collection entered data provider's
               database. -->
          <InsertTime>
            <xsl:apply-templates select="./DIF_Creation_Date" /><xsl:text> 00:00:00</xsl:text>
          </InsertTime>

          <!-- The most recent date an update occurred in the data provider's
               database. -->
          <LastUpdate>
            <xsl:apply-templates select="./Last_DIF_Revision_Date" /><xsl:text> 00:00:00</xsl:text>
          </LastUpdate>

          <!-- The date the collection is or is planned to be deleted from
               the data provider's database. -->
          <!-- We comment this out, to prevent a collection from inadvertently
               being deleted when it is present and has certain values. -->
          <!-- <DeleteTime/> -->
          <!-- End No Mapping -->

          <!-- ECHO LongName, the reference name used in describing the
               scientific contents of the data collection (required) -->
          <!-- Max. length of Data_Set_Citation/Dataset_Title is 80 chars.
               There can be more than one Data_Set_Citation in the DIF.
               Max. length of ECHO Longname is 1024 chars. -->
          <LongName>
            <xsl:value-of select="./Data_Set_Citation/Dataset_Title" />
          </LongName>

          <!-- ECHO DataSetID, a unique name for the data collection
               (required) -->
          <!-- Max. length of Data_Set_Citation/Dataset_Title is 80 chars.
               Max. length of Data_Set_Citation/Version is 80 chars.
               There can be more than one Data_Set_Citation in the DIF.
               Max. length of ECHO DataSetID is 500 chars. -->
          <DataSetID><xsl:value-of select="./Data_Set_Citation/Dataset_Title" /><xsl:text> V</xsl:text><xsl:value-of select="./Data_Set_Citation/Version" /></DataSetID>

          <!-- The major emphasis of the content of the collection
               (required) -->
          <!-- Max. length of ECHO CollectionDescription is 4000 chars. -->
          <CollectionDescription>
            <xsl:apply-templates select="./Summary" />
          </CollectionDescription>

          <RevisionDate>
            <xsl:apply-templates select="./Last_DIF_Revision_Date" /><xsl:text> 00:00:00</xsl:text>
          </RevisionDate>

          <!-- No DIF Mapping
          <SuggestedUsage1/>
          <SuggestedUsage2/>
          End No Mapping -->

          <!-- Center where collection was or is being processed.-->
          <!-- Max. length of Originating_Center is 80 chars.
               Max. length of ECHO ProcessingCenter is 80 chars. -->
          <ProcessingCenter>
            <xsl:apply-templates select="./Originating_Center" />
          </ProcessingCenter>

          <!-- No DIF Mapping
          <ProcessingLevelId/>
          <ProcessingLevelDescription/>
          End No Mapping -->

          <!-- Center where collection is archived -->
          <!-- Max. length of Data_Center_Name/Short_Name is 160 chars.
               There can be more than one Data_Center in the DIF.
               Max. length of ECHO ArchiveCenter is 80 chars. -->
          <ArchiveCenter>
            <xsl:apply-templates select="./Data_Center/Data_Center_Name/Short_Name" mode="ArchiveCenter" />
          </ArchiveCenter>

          <!-- No DIF Mapping
          <VersionDescription/>
          End No Mapping -->

          <!-- ECHO CitationforExternalPublication -->
          <!-- There can be more than one Data_Set_Citation in the DIF. -->
          <!-- Max. length of CitationforExternalPublication is 4000 chars. -->
          <CitationforExternalPublication>
            <xsl:apply-templates select="Data_Set_Citation" />
          </CitationforExternalPublication>

          <!-- Max. length of Data_Set_Progress is 31 chars.
               Max. length of ECHO CollectionState is 80 chars. -->
          <CollectionState>
            <xsl:apply-templates select="./Data_Set_Progress" />
          </CollectionState>

          <!-- No DIF Mapping
          <MaintenanceandUpdateFrequency/>
          End No Mapping -->

          <RestrictionFlag>0</RestrictionFlag>

          <!-- DIF Access_Constraints -->
          <!-- Max. length of Access_Constraints is 80 chars. per line,
               no limit on number of lines.
               Max. length of ECHO RestrictionComment is 1024 chars. -->
          <xsl:apply-templates select="./Access_Constraints" />

          <!-- DIF Distribution Fee -->
          <Price>0</Price>

          <!-- ======================== Spatial ==========================  -->
          <xsl:choose>
            <xsl:when test="(ShortName/text() = 'OMAERUV') or (ShortName/text() = 'OMBRO') or (ShortName/text() = 'OMCLDO2') or (ShortName/text() = 'OMCLDRR') or (ShortName/text() = 'OMDOAO3') or (ShortName/text() = 'OMHCHO') or (ShortName/text() = 'OMNO2') or (ShortName/text() = 'OMOCLO') or (ShortName/text() = 'OMSO2') or (ShortName/text() = 'OMTO3') or (ShortName/text() = 'OMAERO') or (ShortName/text() = 'OMAEROZ') or (ShortName/text() = 'OMPROO3') or (ShortName/text() = 'OMUVB') or (ShortName/text() = 'OML1BCAL') or (ShortName/text() = 'OML1BIRR') or (ShortName/text() = 'OML1BRUG') or (ShortName/text() = 'OML1BRUZ') or (ShortName/text() = 'OML1BRVG') or (ShortName/text() = 'OML1BRVZ') or (ShortName/text() = 'OMO3PR')">
              <!-- Special case providing orbital spatial coverage fo
                   particular OMI ShortNames -->
              <Spatial>
                <SpatialCoverageType>Orbit</SpatialCoverageType>
                <OrbitParameters>
                     <SwathWidth>2600</SwathWidth>
                     <Period>100</Period>
                     <InclinationAngle>98.2</InclinationAngle>
                </OrbitParameters>
                <GranuleSpatialRepresentation>
                  <Orbit />
                </GranuleSpatialRepresentation>
              </Spatial>
            </xsl:when>
            <xsl:when test="(ShortName/text() = 'AIRABRAD') or (ShortName/text() = 'AIRHBRAD') or (ShortName/text() = 'AIRIBQAP') or (ShortName/text() = 'AIRIBRAD') or (ShortName/text() = 'AIRVBQAP') or (ShortName/text() = 'AIRVBRAD') or (ShortName/text() = 'TOMSEPL2') or (ShortName/text() = 'TOMSN7L2') or (ShortName/text() = 'TOMSN7L1BRAD')">
              <!-- Special case providing Vertical spatial coverage for
                   particular ShortNames -->
              <Spatial>
                <SpatialCoverageType>Vertical</SpatialCoverageType>
                <VerticalSpatialDomain>
                  <VerticalSpatialDomainType>Minimum Altitude</VerticalSpatialDomainType>
                  <VerticalSpatialDomainValue>SFC</VerticalSpatialDomainValue>
                </VerticalSpatialDomain>
                <VerticalSpatialDomain>
                  <VerticalSpatialDomainType>Maximum Altitude</VerticalSpatialDomainType>
                  <VerticalSpatialDomainValue>TOA</VerticalSpatialDomainValue>
                </VerticalSpatialDomain>
                <GranuleSpatialRepresentation>
                  <Geodetic />
                </GranuleSpatialRepresentation>
              </Spatial>
            </xsl:when>
            <xsl:when test="Spatial_Coverage">
              <!-- General case of Horizontal spatial coverage -->
              <Spatial>
                <SpatialCoverageType>Horizontal</SpatialCoverageType>
                <HorizontalSpatialDomain>
                  <!-- No DIF Mapping
                       <ZoneIdentifier/>
                       End No Mapping -->
                  <Geometry>
                    <!-- *** Polygon fields commented out. No Mapping from DIF ***
                    <CoordinateSystem>
                      Cartesian/Geodetic
                    </CoordinateSystem>
                    <Point>
                      <PointLongitude/>
                      <PointLatitude/>
                    </Point>
                    <Circle>
                      <CenterLatitude/>
                      <CenterLongitude/>
                      <Radius/>
                    </Circle>
                    -->
                    <!-- ECHO Bounding Rectangle mapped from DIF Spatial Coverage -->
                    <xsl:apply-templates select="Spatial_Coverage" />
                    <!--
                    <GPolygon>
                      <Boundary>
                        <Point/>
                        <Point/>
                        <Point/>
                        <Point/>
                      </Boundary>
                      <ExclusiveZone>
                        <Boundary/>
                      </ExclusiveZone>
                    </GPolygon>
                    <Polygon>
                      <SinglePolygon>
                        <OutRing>
                          <Boundary>
                            <Point/>
                            <Point/>
                            <Point/>
                            <Point/>
                          </Boundary>
                        </OutRing>
                        <InnerRing>
                          <Boundary>
                            <Point/>
                            <Point/>
                            <Point/>
                            <Point/>
                          </Boundary>
                        </InnerRing>
                      </SinglePolygon>
                      <MultiPolygon>
                        <SinglePolygon>
                          <OutRing>
                            <Boundary>
                              <Point/>
                              <Point/>
                              <Point/>
                              <Point/>
                            </Boundary>
                          </OutRing>
                          <InnerRing>
                            <Boundary>
                              <Point/>
                              <Point/>
                              <Point/>
                              <Point/>
                            </Boundary>
                          </InnerRing>
                        </SinglePolygon>
                      </MultiPolygon>
                    </Polygon>
                    <Line>
                      <Point/>
                    </Line>
                    -->
                  </Geometry>
                </HorizontalSpatialDomain>

                <!-- No DIF Mapping
                <VerticalSpatialDomain>
                  <VerticalSpatialDomainType/>
                  <VerticalSpatialDomainValue/>
                </VerticalSpatialDomain>
                     End No Mapping -->

              </Spatial>
            </xsl:when>
            <xsl:otherwise>
              <!-- For the case where the DIF has no spatial_coverage section,
                   this Spatial section of the ECHO collection metadata will
                   indicate that there will be no SpatialDomainContainer in
                   the granule metadata. By doing this, we assume that
                   whenever the DIF has no spatial_coverage section, the
                   granule metadata will have no SpatialDomainContainer. -->
              <Spatial>
                <SpatialCoverageType>Horizontal</SpatialCoverageType>
                <GranuleSpatialRepresentation>
                  <NoSpatial />
                </GranuleSpatialRepresentation>
              </Spatial>
            </xsl:otherwise>
          </xsl:choose>

          <!-- Orbit Parameters, GranuleSpatialRepresentation, and
               GranuleSpatialInheritance were skipped. There is no mapping
               from DIF fields -->
          <!-- ==================== End Spatial ==========================  -->

          <!-- ==================== Temporal =============================  -->
          <Temporal>
            <!-- No DIF Mapping
            <TimeType/>
            <DateType/>
            <TemporalRangeType/>
            <PrecisionofSeconds/>
            <EndsatPresentFlag/>
            End No Mapping -->
            <RangeDateTime>
              <!-- ECHO RangeBeginningDate -->
              <xsl:apply-templates select="./Temporal_Coverage/Start_Date" />
              <!-- ECHO RangeEndingDate -->
              <xsl:apply-templates select="./Temporal_Coverage/Stop_Date" />
            </RangeDateTime>
            <!--
            <SingleDateTime>
              <CalendarDate/>
              <TimeofDay/>
            </SingleDateTime>
            -->
            <!--
            <PeriodicDateTime>
              <PeriodName/>
              <Period1stDate/>
              <Period1stTime/>
              <PeriodEndDate/>
              <PeriodEndTime/>
              <PeriodDurationUnit/>
              <PeriodDurationValue/>
              <PeriodCycleDurationUnit/>
              <PeriodCycleDurationValue/>
            </PeriodicDateTime>
            -->
          </Temporal>
          <!-- ==================== End Temporal =========================  -->

          <!-- ==================== Begin Contact ========================  -->
          <xsl:apply-templates select="Personnel" />
          <!-- ==================== End Contact ==========================  -->

          <!-- ==================== Begin Science Keywords ===============  -->
          <xsl:if test="Parameters">
            <ScienceKeywords>
              <xsl:apply-templates select="Parameters" />
            </ScienceKeywords>
          </xsl:if>
          <!-- ==================== End Science Keywords =================  -->

          <!-- ==================== Begin Platform-Instrument ============  -->
          <xsl:apply-templates select="Source_Name" />
          <!-- <xsl:apply-templates select="Sensor_Name" /> -->
          <!-- ==================== End Platform-Instrument ==============  -->

          <!-- ==================== Begin Additional Attributes ==========  -->
          <!-- GCMD Data_Resolution is mapped to ECHO spatialinfo and not
               MeasurementResolution -->
          <!--
          <AdditionalAttributes>
            <AdditionalAttributeDataType/>
            <AdditionalAttributeDescription/>
            <AdditionalAttributeName/>
            <MeasurementResolution/>
            <ParameterRangeBegin/>
            <ParameterRangeEnd/>
            <ParameterUnitsOfMeasure/>
            <ParameterValueAccuracy/>
            <ValueAccuracyExplanation/>
          </AdditionalAttributes>
          -->
          <!-- The only AdditionalAttributes case for now is when a
               Related_URL specifies an OPeNDAP server -->
          <xsl:choose>
            <xsl:when test="Related_URL[(URL_Content_Type/Type = 'GET DATA') and (URL_Content_Type/Subtype = 'OPENDAP DIRECTORY (DODS)')]">
              <AdditionalAttributes>
                <xsl:apply-templates select="Related_URL[(URL_Content_Type/Type='GET DATA') and (URL_Content_Type/Subtype='OPENDAP DIRECTORY (DODS)')]" />
              </AdditionalAttributes>
            </xsl:when>
          </xsl:choose>
          <!-- ==================== End Additional Attributes ============  -->

          <!-- ==================== Begin Spatial Keyword  ===============  -->
          <xsl:apply-templates select="Location" />
          <!-- ==================== End Spatial Keyword  =================  -->

          <!-- ==================== Temporal Keyword =====================  -->
          <!-- No DIF mapping to ECHO Temporal_Keyword field
          <TemporalKeyword/>
          End No Mapping -->

          <!-- ==================== End Temporal Keyword =================  -->

          <!-- ==================== Begin CSDT Description ===============  -->
          <!-- No DIF mapping
          <CSDTDescription>
            <PrimaryCSDT/>
            <Implementation/>
            <CSDTComments/>
            <IndirectReference/>
          </CSDTDescription>
          End No Mapping -->
          <!-- ==================== End CSDT Description =================  -->

          <!-- Skipped CollectionAssociation - no DIF mapping  -->

          <!-- ==================== Begin Campaign  ======================  -->
          <xsl:apply-templates select="Project" />
          <!-- ==================== End Campaign =========================  -->

          <!-- Skipped AlgorithmPackage - No DIF mapping  -->

          <!-- ==================== Begin SpatialInfo ====================  -->
          <!--  DIF Data Resolution field is mapped to spatialinfo field  -->
          <!-- No DIF mapping
          <SpatialInfo>
            <SpatialCoverage_Type/>
            <AltitudeDatumName/>
            <AltitudeDistanceUnits/>
            <AltitudeEncodingMethod/>
            <DepthDatumName/>
            <DepthDistanceUnits/>
            <DepthEncodingMethod/>
            <DenominatorofFlatteningRatio/>
            <EllipsoidName/>
            <HorizontalDatumName/>
            <SemiMajorAxis/>
            <GeographicCoordinateUnits/>
            <LatitudeResolution/>
            <LongitudeResolution/>

            <LocalCoordinateSystemDesc/>
            <LocalGeoReferenceInformation/>
            <PlanarCoordinateSystem/>

            <DepthResolution/>
            <AltitudeResolution/>
          </SpatialInfo>
          End No Mapping -->
          <!-- ==================== End SpatialInfo ======================  -->

          <!-- A null value is printed in the output if the Related_URL group
               does not exist in the DIF. If the Related_URL has a
               URL_Content_Type of 'GET DATA' it is mapped to an
               OnlineAccessURLs Group unless its Subtype is
               'OPENDAP DIRECTORY (DODS)'; all other URL_Content_Type
               are mapped to CollectionOnlineResources -->

          <!-- ================ Begin OnlineAccessURLs ============ -->
          <!-- Avoid including OPENDAP URLs, because they belong in
               AdditionalAttributes -->
          <xsl:choose>
            <xsl:when test="Related_URL[(URL_Content_Type/Type = 'GET DATA') and ((not(URL_Content_Type/Subtype)) or (URL_Content_Type/Subtype != 'OPENDAP DIRECTORY (DODS)'))]">
              <OnlineAccessURLs>
                <xsl:apply-templates select="Related_URL[(URL_Content_Type/Type='GET DATA') and ((not(URL_Content_Type/Subtype)) or (URL_Content_Type/Subtype!='OPENDAP DIRECTORY (DODS)'))]" />
              </OnlineAccessURLs>
            </xsl:when>
          </xsl:choose>
          <!-- ================ End OnlineAccessURLs ============== -->

          <!-- ================== Begin CollectionOnlineResources ===== -->
          <xsl:choose>
            <xsl:when test='Related_URL[(URL_Content_Type/Type != "GET DATA")]'>
              <CollectionOnlineResources>
                <xsl:apply-templates select='Related_URL[(URL_Content_Type/Type!="GET DATA")]' />
              </CollectionOnlineResources>
            </xsl:when>
          </xsl:choose>
          <!-- ================== End CollectionOnlineResources ======= -->

          <!-- ================== Begin DataFormat ======================== -->
          <!-- Max. length of Distribution/Distribution_Format is 80 chars.
               There can be more than one Distribution group in the DIF.
               Max. length of ECHO DataFormat is 80 chars. -->
          <DataFormat>
            <xsl:apply-templates select="./Distribution/Distribution_Format" />
          </DataFormat>
          <!-- ================== End DataFormat ========================== -->

          <!-- Skipped Orderable - no DIF mapping  -->

          <!-- ================== Begin Associated DIFs =================== -->
          <AssociatedDIFs>
            <DIF>
              <EntryID>
                <xsl:apply-templates select="Entry_ID" />
              </EntryID>
            </DIF>
          </AssociatedDIFs>
          <!-- ================== End Associated DIFs ===================== -->

        </CollectionMetaData>
      </Collections>
    </CollectionMetaDataSets>
  </CollectionMetaDataFile>
</xsl:template>

<!-- ================== End S4PACollectionMetaDataFile ==================== -->
<!-- ====================================================================== -->


<!-- ======================= Templates start here ========================= -->

<xsl:template match="Data_Set_Citation">
  <xsl:apply-templates select="Dataset_Creator" />
  <xsl:apply-templates select="Dataset_Release_Date" />
  <xsl:apply-templates select="Dataset_Title" />
</xsl:template>

<xsl:template match="Access_Constraints">
          <RestrictionComment><xsl:value-of select="." /></RestrictionComment>
</xsl:template>

<xsl:template match="Dataset_Creator">
  <xsl:text>&#xA; ORIGINATOR: </xsl:text>
  <xsl:value-of select="normalize-space(.)" />
</xsl:template>

<xsl:template match="Dataset_Release_Date">
  <xsl:text>&#xA; PUBLICATION DATE: </xsl:text>
  <xsl:value-of select="normalize-space(.)" />
</xsl:template>

<xsl:template match="Dataset_Title">
  <xsl:text>&#xA; TITLE: </xsl:text>
  <xsl:value-of select="normalize-space(.)" />
</xsl:template>

<xsl:template match="Entry_ID">
  <xsl:value-of select="." />
</xsl:template>

<!-- Summary has maximum length of 4000 -->
<!--
<xsl:template match="Summary">
  <xsl:choose>
    <xsl:when test="string-length(.) &lt; 3998">
      <xsl:value-of select="." />
    </xsl:when>
    <xsl:otherwise>
      <xsl:value-of select="substring(.,1,3997)" />
      <xsl:text>...</xsl:text>
    </xsl:otherwise>
  </xsl:choose>
</xsl:template>
-->

<xsl:template match="Related_URL">
  <xsl:choose>
    <xsl:when test="URL_Content_Type/Type/text() = 'GET DATA'">
      <xsl:choose>
        <xsl:when test="URL_Content_Type/Subtype/text() = 'OPENDAP DIRECTORY (DODS)'">
          <!-- DIF Related_URL GET DATA specifies an OPeNDAP server -->
          <AdditionalAttributeDataType>STRING</AdditionalAttributeDataType>
          <AdditionalAttributeDescription>
            <xsl:apply-templates select="Description" />
          </AdditionalAttributeDescription>
          <AdditionalAttributeName>OPeNDAPServer</AdditionalAttributeName>
          <AdditionalAttributeValue>
            <xsl:apply-templates select="URL" />
          </AdditionalAttributeValue>
        </xsl:when>
        <xsl:otherwise>
          <!-- DIF Related_URL GET DATA is ECHO OnlineAccessUrl -->
          <xsl:for-each select="URL">
            <OnlineAccessURL>
              <URL>
                <xsl:value-of select="." />
              </URL>
              <URLDescription>
                <xsl:apply-templates select="../Description[/DIF/Related_URL/URL_Content_Type/Type='GET DATA']" />
              </URLDescription>
              <!-- No DIF Mapping
              <MimeType/>
              End No Mapping -->
            </OnlineAccessURL>
          </xsl:for-each>
        </xsl:otherwise>
      </xsl:choose>
    </xsl:when>
    <xsl:otherwise>
      <xsl:for-each select="URL">
        <OnlineResource>
          <OnlineResourceURL>
            <xsl:value-of select="." />
          </OnlineResourceURL>
          <OnlineResourceDescription>
            <xsl:apply-templates select="../Description" />
          </OnlineResourceDescription>
          <OnlineResourceType>
            <xsl:apply-templates select="../URL_Content_Type" />
          </OnlineResourceType>
          <!-- No DIF Mapping
          <OnlineResourceMimeType/>
          End No Mapping -->
        </OnlineResource>
      </xsl:for-each>
    </xsl:otherwise>
  </xsl:choose>
</xsl:template>

<xsl:template match="URL">
  <xsl:value-of select="." />
</xsl:template>

<xsl:template match="Description">
  <xsl:value-of select="." />
</xsl:template>

<xsl:template match="URL_Content_Type">
  <xsl:value-of select="Type" />
  <xsl:if test = "Subtype">
    <xsl:text> : </xsl:text>
    <xsl:value-of select="Subtype" />
  </xsl:if>
</xsl:template>

<!-- DIF Project is ECHO Campaign -->

<xsl:template match="Project">
  <Campaign>
    <CampaignShortName>
      <xsl:value-of select="Short_Name" />
    </CampaignShortName>
    <CampaignLongName>
      <xsl:apply-templates select="Long_Name" />
    </CampaignLongName>
    <!-- No DIF Mapping
    <CampaignStartDate/>
    <CampaignEndDate/>
    End No Mapping -->
  </Campaign>
</xsl:template>

<!-- Expect this template to be used only when
     /Data_Center/Data_Center_Name/Short_Name has been selected for
     ECHO DataCenterId.
     Maximum length of DataCenterID is 80 characters.
-->
<xsl:template match="Short_Name" mode="DataCenterId">
  <xsl:value-of select="." />
  <xsl:if test = "not(position()=last())" >
    <xsl:text>, </xsl:text>
  </xsl:if>
</xsl:template>

<!-- Expect this template to be used only when
     /Data_Center/Data_Center_Name/Short_Name has been selected for
     ECHO ArchiveCenter.
     Maximum length of DataCenterID is 80 characters.
-->
<xsl:template match="Short_Name" mode="ArchiveCenter">
  <xsl:choose>
    <xsl:when test="./text () = 'NASA/GSFC/ESED/GCDC/GES-DISC/DAAC'">NASA/GSFC/GES-DISC-DAAC</xsl:when>
    <xsl:otherwise>
      <xsl:value-of select="." />
    </xsl:otherwise>
  </xsl:choose>
  <xsl:if test = "not(position()=last())" >
    <xsl:text>, </xsl:text>
  </xsl:if>
</xsl:template>

<xsl:template match="Long_Name">
  <xsl:value-of select="." />
</xsl:template>

<!-- DIF Location is ECHO Spatial Keyword -->

<xsl:template match="Location">
  <xsl:apply-templates select="Location_Category" />
  <xsl:apply-templates select="Location_Type" />
  <xsl:apply-templates select="Location_Subregion1" />
  <xsl:apply-templates select="Detailed_Location" />
</xsl:template>

<xsl:template match="Location_Category">
  <SpatialKeyword>
    <xsl:value-of select="." />
  </SpatialKeyword>
</xsl:template>

<xsl:template match="Location_Type">
  <SpatialKeyword>
    <xsl:value-of select="." />
  </SpatialKeyword>
</xsl:template>

<xsl:template match="Location_Subregion1">
  <SpatialKeyword>
    <xsl:value-of select="." />
  </SpatialKeyword>
</xsl:template>

<xsl:template match="Detailed_Location">
  <SpatialKeyword>
    <xsl:value-of select="." />
  </SpatialKeyword>
</xsl:template>

<!-- DIF Parameters field is ECHO DisciplineTopicParameters -->

<xsl:template match="Parameters">
  <ScienceKeyword>
    <xsl:apply-templates select="Category" />
    <xsl:apply-templates select="Topic" />
    <xsl:apply-templates select="Term" />
    <xsl:apply-templates select="Variable_Level_1" />
    <xsl:apply-templates select="Detailed_Variable" />
  </ScienceKeyword>
</xsl:template>

<xsl:template match="Category">
  <CategoryKeyword>
    <xsl:value-of select="." />
  </CategoryKeyword>
</xsl:template>

<xsl:template match="Topic">
  <TopicKeyword>
    <xsl:value-of select="." />
  </TopicKeyword>
</xsl:template>

<xsl:template match="Term">
  <TermKeyword>
    <xsl:value-of select="." />
  </TermKeyword>
</xsl:template>

<xsl:template match="Variable_Level_1">
  <VariableLevel1Keyword>
    <Value><xsl:value-of select="." /></Value>
    <xsl:apply-templates select="../Variable_Level_2" />
  </VariableLevel1Keyword>
</xsl:template>

<xsl:template match="Variable_Level_2">
  <VariableLevel2Keyword>
    <Value><xsl:value-of select="." /></Value>
    <xsl:apply-templates select="../Variable_Level_3" />
  </VariableLevel2Keyword>
</xsl:template>

<xsl:template match="Variable_Level_3">
  <VariableLevel3Keyword>
    <xsl:value-of select="." />
  </VariableLevel3Keyword>
</xsl:template>

<xsl:template match="Detailed_Variable">
  <DetailedVariableKeyword>
    <xsl:value-of select="." />
  </DetailedVariableKeyword>
</xsl:template>

<!-- DIF Personnel Field -->

<xsl:template match="Personnel">
  <Contact>
    <Role><xsl:apply-templates select="Role" /></Role>
    <!-- No DIF Mapping
    <HoursOfService/>
    <ContactInstructions/>
    End No Mapping -->

    <!-- Inserts DIF Data Center Name into ECHO ContactOrganizationName
         for a contact whose role is Data Center Contact -->
    <xsl:choose>
      <xsl:when test="Personnel[Role='DATA CENTER CONTACT']">
        <ContactOrganizationName>
          <xsl:apply-templates select="Data_Center_Name" />
        </ContactOrganizationName>
      </xsl:when>
      <!--
      <xsl:otherwise>
        <ContactOrganizationName/>
      </xsl:otherwise>
      -->
    </xsl:choose>

    <xsl:apply-templates select="Contact_Address" />
    <xsl:apply-templates select="Phone" />
    <xsl:apply-templates select="Fax" />
    <xsl:apply-templates select="Email" />
    <ContactPersons>

      <!-- added to look for missing First_Name -->
      <xsl:choose>
        <xsl:when test="./First_Name">
          <xsl:apply-templates select="First_Name" />
        </xsl:when>
        <xsl:otherwise>
          <ContactFirstName>PLEASE CONTACT</ContactFirstName>
        </xsl:otherwise>
      </xsl:choose>
      <!-- end of looking for missing First_Name -->

      <xsl:apply-templates select="Middle_Name" />
      <xsl:apply-templates select="Last_Name" />
      <!-- No DIF Mapping
      <ContactJobPosition/>
      End No Mapping -->
    </ContactPersons>
  </Contact>
</xsl:template>

<xsl:template match="Role">
  <xsl:value-of select="." />
  <xsl:if test = "not(position()=last())" >
    <xsl:text>, </xsl:text>
  </xsl:if>
</xsl:template>

<xsl:template match="Phone">
  <OrganizationTelephone>
    <TelephoneNumber>
      <xsl:value-of select="." />
    </TelephoneNumber>
    <TelephoneType>Direct Line</TelephoneType>
  </OrganizationTelephone>
</xsl:template>

<xsl:template match="Fax">
  <OrganizationTelephone>
    <TelephoneNumber>
      <xsl:value-of select="." />
    </TelephoneNumber>
    <TelephoneType>Fax</TelephoneType>
  </OrganizationTelephone>
</xsl:template>

<xsl:template match="Email">
  <OrganizationEmail>
    <ElectronicMailAddress>
      <xsl:value-of select="." />
    </ElectronicMailAddress>
  </OrganizationEmail>
</xsl:template>

<xsl:template match="First_Name">
  <ContactFirstName>
    <xsl:value-of select="." />
  </ContactFirstName>
</xsl:template>

<!-- Provide a default value for First_Name if value is missing -->
<xsl:template match="First_Name[not(text())]">
  <ContactFirstName>PLEASE CONTACT</ContactFirstName>
</xsl:template>

<xsl:template match="Middle_Name">
  <ContactMiddleName>
    <xsl:value-of select="." />
  </ContactMiddleName>
</xsl:template>

<xsl:template match="Last_Name">
  <ContactLastName>
    <xsl:value-of select="." />
  </ContactLastName>
</xsl:template>

<xsl:template match="Data_Center_Name">
  <xsl:value-of select="." />
</xsl:template>

<xsl:template match="Contact_Address">
  <!-- Do not include ContactOrganizationAddress unless all fields are present -->
  <xsl:choose>
    <xsl:when test="./Address and ./City and ./Province_or_State and ./Postal_Code and ./Country">
      <ContactOrganizationAddress>
        <StreetAddress>
          <xsl:apply-templates select="Address" />
        </StreetAddress>
        <xsl:apply-templates select="City" />
        <xsl:apply-templates select="Province_or_State" />
        <xsl:apply-templates select="Postal_Code" />
        <xsl:apply-templates select="Country" />
      </ContactOrganizationAddress>
    </xsl:when>
  </xsl:choose>
</xsl:template>

<!-- Separated multiple occurrences by ', ' EJS 6-6-2006 -->
<xsl:template match="Address">
  <xsl:value-of select="." />
  <xsl:if test = "not(position()=last())" >
    <xsl:text>, </xsl:text>
  </xsl:if>
</xsl:template>

<xsl:template match="City">
  <City>
    <xsl:value-of select="." />
  </City>
</xsl:template>

<xsl:template match="Province_or_State">
  <StateProvince>
    <xsl:value-of select="." />
  </StateProvince>
</xsl:template>

<xsl:template match="Postal_Code">
  <PostalCode>
    <xsl:value-of select="." />
  </PostalCode>
</xsl:template>

<xsl:template match="Country">
  <Country>
    <!-- xsl:value-of select="." /-->
    <!-- ECHO has a maximum length of 10 for Country -->
    <xsl:value-of select="substring(.,1,10)" />
  </Country>
</xsl:template>

<!-- End Personnel -->


<!-- Duplicate commented out by EJS
<xsl:template match="Long_Name">
  <xsl:value-of select="." />
</xsl:template>
END Duplicate commented out by EJS -->

<xsl:template match="Sensor_Name">
  <xsl:variable name="shortName" select="Short_Name" />
  <Platform>
    <PlatformShortName/>
    <PlatformLongName/>
    <PlatformType/>
    <!-- No DIF Mapping
    <PlatformCharacteristic>
      <PlatformCharacteristicName/>
      <PlatformCharacteristicDescription/>
      <PlatformCharacteristicDataType/>
      <PlatformCharacteristicUnit/>
      <PlatformCharacteristicValue/>
    </PlatformCharacteristic>
    End No Mapping -->

    <Instrument>
      <!--
      <InstrumentShortName>
        <xsl:value-of select="substring-before($shortName, '-')" />
      </InstrumentShortName> -->
      <InstrumentShortName/>
      <Sensor>
        <SensorShortName>
          <!-- <xsl:value-of select="substring-after($shortName, '-')" /> -->
          <xsl:value-of select="Short_Name" />
        </SensorShortName>
        <SensorLongName>
          <xsl:value-of select="Long_Name" />
        </SensorLongName>
      </Sensor>
    </Instrument>
  </Platform>
</xsl:template>

<!-- Duplicate commented out by EJS
<xsl:template match="Short_Name">
  <xsl:value-of select="." />
</xsl:template>
END Duplicate commented out by EJS -->

<!--
ECHO allows more than one platform, expects each platform to contain
0 or more instruments, and each instrument to contain 0 or more sensors.
The DIF does not contain sensors (Sensor_Name) in platforms (Source_Name),
instead it just lists all platforms (sources) and sensors. It is
possible for different sensors to be associated with different platforms,
but it is impossible to determine the pairing between sensors and platforms
from the DIF.
Rather than associate all sensors with each platform, for the time being
the ECHO collection metadata will only be populated with the first
platform (source) encountered in the DIF, and that platform will be populated
with and instrument and sensor obtained from the first sensor encountered
in the DIF.
If the Short_Name is 'MODELS', then the Platform section is not relevant,
so don't include it.
-->
<xsl:template match="Source_Name">
  <xsl:if test = "(position()=1) and (Short_Name/text() != 'MODELS')" >
  <Platform>
    <PlatformShortName><xsl:value-of select="Short_Name" /></PlatformShortName>
    <PlatformLongName><xsl:value-of select="Long_Name" /></PlatformLongName>
    <PlatformType>Not Specified</PlatformType>
    <Instrument>
      <InstrumentShortName><xsl:value-of select="//Sensor_Name/Short_Name" /></InstrumentShortName>
      <Sensor>
        <SensorShortName><xsl:value-of select="//Sensor_Name/Short_Name" /></SensorShortName>
      </Sensor>
    </Instrument>
  </Platform>
  </xsl:if>
</xsl:template>
<!--
If we wanted all platforms to be populated with all sensors, this is how we
would do it.
<xsl:template match="Source_Name">
  <Platform>
    <PlatformShortName><xsl:value-of select="Short_Name" /></PlatformShortName>
    <PlatformLongName><xsl:value-of select="Long_Name" /></PlatformLongName>
    <PlatformType>Not Specified</PlatformType>
    <xsl:for-each select="//Sensor_Name">
    <Instrument>
      <InstrumentShortName><xsl:value-of select="./Short_Name" /></InstrumentShortName>
      <Sensor>
        <SensorShortName><xsl:value-of select="./Short_Name" /></SensorShortName>
      </Sensor>
    </Instrument>
    </xsl:for-each>
  </Platform>
</xsl:template>
-->

<!-- ECHO TemporalCoverage field
<xsl:template match="Temporal_Coverage">
  <TemporalCoverage>
    <xsl:apply-templates select="Start_Date" />
    <xsl:apply-templates select="Stop_Date" />
  </TemporalCoverage>
</xsl:template>
-->

<xsl:template match="Start_Date">
  <RangeBeginningDate>
    <xsl:value-of select="." />
  </RangeBeginningDate>
  <RangeBeginningTime>00:00:00.0000000</RangeBeginningTime>
</xsl:template>

<xsl:template match="Stop_Date">
  <RangeEndingDate>
    <xsl:value-of select="." />
  </RangeEndingDate>
  <RangeEndingTime>23:59:59.9999999</RangeEndingTime>
</xsl:template>
<!-- End TemporalCoverage field -->

<!-- Spatial Coverage -->
<xsl:template match="Spatial_Coverage">
  <BoundingRectangle>
    <xsl:apply-templates select="Westernmost_Longitude" />
    <xsl:apply-templates select="Northernmost_Latitude" />
    <xsl:apply-templates select="Easternmost_Longitude" />
    <xsl:apply-templates select="Southernmost_Latitude" />
  </BoundingRectangle>
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

<xsl:template match="Minimum_Altitude">
  <xsl:value-of select="." />
</xsl:template>

<xsl:template match="Maximum_Altitude">
  <xsl:value-of select="." />
</xsl:template>

<xsl:template match="Minimum_Depth">
  <xsl:value-of select="." />
</xsl:template>

<xsl:template match="Maximum_Depth">
  <xsl:value-of select="." />
</xsl:template>
<!-- End Spatial Coverage -->

<xsl:template match="Latitude_Resolution">
  <xsl:value-of select="." />
</xsl:template>

<xsl:template match="Longitude_Resolution">
  <xsl:value-of select="." />
</xsl:template>

<xsl:template match="Altitude_Resolution">
  <xsl:value-of select="." />
</xsl:template>

<xsl:template match="Depth_Resolution">
  <xsl:value-of select="." />
</xsl:template>

<xsl:template match="Temporal_Resolution">
  <TemporalResolution><xsl:value-of select="." /></TemporalResolution>
</xsl:template>

</xsl:stylesheet>
