<?xml version="1.0" encoding="UTF-8"?>
<!-- $Id: S4paDIF2ECHO10.xsl,v 1.33 2015/10/30 17:52:49 mtheobal Exp $ -->
<!-- -@@@ S4PA, Version $Name:  $ -->
<xsl:stylesheet 
xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0" 
xmlns:java="http://xml.apache.org/xslt/java" exclude-result-prefixes="java">

<xsl:output method="xml" version="1.0" encoding="UTF-8" indent="yes" />

<!-- This is an XSLT style sheet that transforms
     metadata in DIF XML format to ECHO CollectionMetadata XML format,
     using the ECHO 10 schema.
-->

<!-- apply templates -->

<xsl:template match="DIF">
  <CollectionMetaDataFile xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="http://www.echo.nasa.gov/ingest/schemas/operations/Collection.xsd">
    <Collections>
      <Collection>

        <!-- ECHO ShortName, the official reference name used in
             identifying the contents of the data collection (required).
             All characters must be in upper case. -->
        <!-- Max. length of Entry_ID is 80 characters.
             Max. length of ECHO ShortName is 85 characters. -->
        <ShortName>
           <!-- (We expect ShortName to be inserted into the DIF by
               s4pa_convert_DIF.pl, since ShortName is
               not a field currently defined for a DIF. -->
          <xsl:apply-templates select="./ShortName" />
        </ShortName>

        <!-- ECHO VersionId, the version identifier of the data
             collection (required) -->
        <!-- Max. length of Data_Set_Citation/Version is 80 chars.
             There can be more than one Data_Set_Citation in the DIF.
             Max. length of ECHO VersionId is 80 characters -->
        <VersionId>
          <xsl:apply-templates select="./Data_Set_Citation/Version" />
        </VersionId>

        <!-- The insert date/time the collection entered data provider's
             database. -->
        <!-- No DIF Mapping, so we'll provide an alternate value -->
        <InsertTime>
          <xsl:apply-templates select="./DIF_Creation_Date" /><xsl:text>T00:00:00Z</xsl:text>
        </InsertTime>

        <!-- The most recent date an update occurred in the data provider's
             database. -->
        <LastUpdate>
          <xsl:apply-templates select="./Last_DIF_Revision_Date" /><xsl:text>T00:00:00Z</xsl:text>
        </LastUpdate>

        <!-- The date the collection is or is planned to be deleted from
             the data provider's database. -->
        <!-- We comment this out, to prevent a collection from inadvertently
             being deleted when it is present and is earlier than the
             current date -->
        <!-- <DeleteTime/> -->

        <!-- ECHO LongName, the reference name used in describing the
             scientific contents of the data collection (required) -->
        <!-- Max. length of Data_Set_Citation/Dataset_Title is 80 chars.
             There can be more than one Data_Set_Citation in the DIF.
             Max. length of ECHO LongName is 1024 chars. -->
        <LongName>
          <xsl:value-of select="./Data_Set_Citation/Dataset_Title" />
        </LongName>

        <!-- ECHO DataSetId, a unique name for the data collection
             (required) -->
        <!-- Max. length of Data_Set_Citation/Dataset_Title is 80 chars.
             Max. length of Data_Set_Citation/Version is 80 chars.
             There can be more than one Data_Set_Citation in the DIF.
             Max. length of ECHO DataSetId is 1030 chars. -->
        <DataSetId><xsl:value-of select="./Data_Set_Citation/Dataset_Title" /><xsl:text> V</xsl:text><xsl:value-of select="./Data_Set_Citation/Version" /></DataSetId>

        <!-- The major emphasis of the content of the collection
             (required) -->
        <!-- Max. length of ECHO collection Description is 4000 chars. -->
        <Description>
          <xsl:apply-templates select="./Summary" />
        </Description>

        <!-- Attribute indicating these are non-science-quality NRT products -->
        <xsl:if test="contains(Entry_ID,'_NRT')">
            <CollectionDataType>NEAR_REAL_TIME</CollectionDataType>
        </xsl:if>

        <!-- The indication of whether this collection is orderable. -->
        <Orderable>false</Orderable>

        <!-- The use of the Visible flag was deprecated in ECHO 10.23.
             We are now using an ACL rule in ECHO to control visibility via
             the RestrictionFlag value.
          -->
        <Visible>false</Visible>

        <!-- The date and time that this directory entry was created or
             the latest date and time of its modification or update. -->
        <RevisionDate>
          <xsl:apply-templates select="./Last_DIF_Revision_Date" /><xsl:text>T00:00:00Z</xsl:text>
        </RevisionDate>

        <!-- Describes how this collection or granule may be best used to
             support earth science/global change research.-->
        <!-- No DIF Mapping
        <SuggestedUsage/>
        End No Mapping -->

        <!-- Center where collection was or is being processed.-->
        <!-- Max. length of Originating_Center is 80 chars.
             Max. length of ECHO ProcessingCenter is 80 chars. -->
        <!-- ================== Begin ProcessingCenter ================== -->
        <xsl:apply-templates select="./Originating_Center" />
        <!-- ================== End ProcessingCenter ==================== -->

        <!-- The processing level class contains the level identifier and
             level description of the collection. -->
        <!-- No DIF Mapping
        <ProcessingLevelId/>
        <ProcessingLevelDescription/>
        End No Mapping -->

        <!-- Center where collection is archived -->
        <!-- Max. length of Data_Center_Name/Short_Name is 160 chars.
             There can be more than one Data_Center in the DIF.
             Max. length of ECHO ArchiveCenter is 80 chars. -->
        <ArchiveCenter>
          <xsl:text>GESDISC</xsl:text>
        </ArchiveCenter>

        <!-- A brief description of the differences between this
             collection version and another collection version. -->
        <!-- No DIF Mapping
        <VersionDescription/>
        End No Mapping -->
        
        <!-- ECHO CitationForExternalPublication -->
        <!-- The recommended reference to be used when referring to this
             collection in publications. Its format is free text, but should
             include: Originator (the name of an organization or individual
             that developed the data set, where Editor(s)' names are
             followed by (ed.) and Compiler(s)' names are followed
             by (comp.)); Publication date (the date of publication or
             release of the data set); Title (the name by which document
             can be referenced). -->
        <!-- There can be more than one Data_Set_Citation in the DIF. -->
        <!-- Max. length of CitationForExternalPublication is 4000 chars. -->
        <CitationForExternalPublication>
          <xsl:apply-templates select="Data_Set_Citation" />
        </CitationForExternalPublication>

        <!-- This attribute describes the state of the collection,
             whether it is planned but not yet existent, partially
             complete due to continual additions from remotely sensed
             data/processing/reprocessing, or is considered a complete
             product/dataset. -->
        <!-- Max. length of Data_Set_Progress is 31 chars.
             Max. length of ECHO CollectionState is 80 chars. -->
        <xsl:apply-templates select="./Data_Set_Progress" />

        <!-- The frequency with which changes and additions are made to
             the collection after the initial dataset begins to be
             collected/processed. -->
        <!-- No DIF Mapping
        <MaintenanceandUpdateFrequency/>
        End No Mapping -->

        <!-- A numerical value indicates the type of restriction that
             applies on this collection.
             We are now using an ACL rule in ECHO to control visibility via
             the RestrictionFlag value. A value of 1 indicates that the
             visibility is false, i.e the collection is not visible.
             This value will be changed by s4pa_convert_DIF.pl if the
             collection is configured in s4pa_dif_info.cfg to be visible. -->
        <RestrictionFlag>1</RestrictionFlag>

        <!-- ================== Begin RestrictionComment ================ -->
        <!-- Restrictions and legal prerequisites for accessing the
             collection. These include any access constraints applied to
             assure the protection of privacy or intellectual property,
             and any special restrictions or limitations on obtaining
             the collection. These restrictions differ from Use
             Restrictions in that they only apply to access. -->
        <!-- Max. length of Access_Constraints is 80 chars. per line,
             no limit on number of lines.
             Max. length of ECHO RestrictionComment is 1024 chars. -->
        <xsl:apply-templates select="./Access_Constraints" />
        <!-- ================== End RestrictionComment ================== -->

        <!-- The price for ordering the collection. -->
        <Price>0</Price>

        <!-- Max. length of Distribution/Distribution_Format is 80 chars.
             There can be more than one Distribution group in the DIF.
             Max. length of ECHO DataFormat is 80 chars. -->
        <xsl:apply-templates select="./Distribution/Distribution_Format" />

        <!-- ================== Begin Spatial Keywords ================== -->
        <!-- This attribute specifies a word or phrase which serves to
             summarize the spatial regions covered by the collection.
             It may be repeated if several regions are covered. This often
             occurs when a collection is described as covering some large
             region, and several smaller sub regions within that region. -->
        <xsl:if test="Location">
          <SpatialKeywords>
            <xsl:apply-templates select="Location" />
          </SpatialKeywords>
        </xsl:if>
        <!-- ================== End Spatial Keywords ==================== -->

        <!-- This attribute specifies a word or phrase which serves to
             summarize the temporal characteristics referenced in the
             collection. -->
        <!-- No DIF mapping
        <TemporalKeywords/>
        End No Mapping -->

        <!-- ================== Begin Temporal ========================== -->
        <!-- Can Data_Resolution/Temporal_Resolution and
             Data_Resolution/Temporal_Resolution_Range be mapped to
             TemporalRangeType here? -->
        <Temporal>
          <!-- No DIF Mapping
          <TimeType/>
          <DateType/>
          <TemporalRangeType/>
          <PrecisionOfSeconds/>
          <EndsAtPresentFlag/>
          End No Mapping -->
          <RangeDateTime>
            <!-- ECHO RangeBeginningDate -->
            <xsl:apply-templates select="./Temporal_Coverage/Start_Date" />
            <!-- ECHO RangeEndingDate -->
            <!-- Suppress for NRT products -->
            <xsl:if test="not((contains(Entry_ID,'_NRT')) or (contains(Entry_ID,'ICRAD')))">
               <xsl:apply-templates select="./Temporal_Coverage/Stop_Date" />
            </xsl:if>
          </RangeDateTime>
          <!--
          <SingleDateTime/>
          -->
          <!--
          <PeriodicDateTime>
            <Name/>
            <StartDate/>
            <EndDate/>
            <DurationUnit/>
            <DurationValue/>
            <PeriodCycleDurationUnit/>
            <PeriodCycleDurationValue/>
          </PeriodicDateTime>
          -->
        </Temporal>
        <!-- ================== End Temporal ============================ -->

        <!-- ================== Begin Contacts ========================== -->
        <Contacts>
          <xsl:apply-templates select="Personnel" />
          <xsl:apply-templates select="Data_Center/Personnel" />
        </Contacts>
        <!-- ================== End Contacts ============================ -->

        <!-- ==================== Begin Science Keywords ===============  -->
        <xsl:if test="Parameters">
          <ScienceKeywords>
            <xsl:apply-templates select="Parameters" />
          </ScienceKeywords>
        </xsl:if>
        <!-- ==================== End Science Keywords =================  -->

        <!-- ================== Begin Platforms ========================= -->
        <xsl:apply-templates select="Source_Name" />
        <!-- <xsl:apply-templates select="Sensor_Name" /> -->
        <!-- ================== End Platforms =========================== -->

        <!-- ================== Begin AdditionalAttributes ============== -->
        <!-- GCMD Data_Resolution is mapped to ECHO spatialinfo and not
             MeasurementResolution -->
        <!--
        <AdditionalAttributes>
          <AdditionalAttribute>
            <Name/>
            <DataType/>
            <Description/>
            <MeasurementResolution/>
            <ParameterRangeBegin/>
            <ParameterRangeEnd/>
            <ParameterUnitsOfMeasure/>
            <ParameterValueAccuracy/>
            <ValueAccuracyExplanation/>
            <Value/>
          </AdditionalAttribute>
        </AdditionalAttributes>
        -->
        <!-- The only AdditionalAttributes case for now is when a
             Related_URL specifies an OPeNDAP server -->
        <xsl:choose>
          <xsl:when test="Related_URL[(URL_Content_Type/Type = 'GET DATA') and ((URL_Content_Type/Subtype = 'OPENDAP DIRECTORY (DODS)') or (URL_Content_Type/Subtype = 'OPENDAP DATA (DODS)'))]">
            <AdditionalAttributes>
              <xsl:apply-templates select="Related_URL[(URL_Content_Type/Type='GET DATA') and ((URL_Content_Type/Subtype='OPENDAP DIRECTORY (DODS)') or (URL_Content_Type/Subtype = 'OPENDAP DATA (DODS)'))]" />
            </AdditionalAttributes>
          </xsl:when>
        </xsl:choose>
        <!-- ================== End AdditionalAttributes ================ -->

        <!-- ================== Begin CSDTDescriptions ================== -->
        <!-- This entity stores the description of the data organization
             of the collection (i.e. a generalized collection Description
             in terms of internal structure). -->
        <!-- No DIF mapping
        <CSDTDescriptions>
          <CSDTDescription>
            <PrimaryCSDT/>
            <Implementation/>
            <CSDTComments/>
            <IndirectReference/>
          </CSDTDescription>
        </CSDTDescriptions>
        End No Mapping -->
        <!-- ================== End CSDTDescriptions ==================== -->

        <!-- This entity is used to describe collections associated with 
             the instance of a collection; i.e., the name and other details
             of input collections, collections associated (in science data
             terms) with the instance and/or collections dependent on the
             collection in some way. -->
        <!-- No DIF mapping
        <CollectionAssociation>
          <ShortName></ShortName>
          <VersionId></VersionId>
          <CollectionType></CollectionType>
          <CollectionUse></CollectionUse>
        </CollectionAssociation>
        End No Mapping -->
        
        <!-- ================== Begin Campaigns  ======================== -->
        <xsl:if test="Project">
          <Campaigns>
            <xsl:apply-templates select="Project" />
          </Campaigns>
        </xsl:if>
        <!-- ================== End Campaigns =========================== -->

        <!-- This entity provides the common characteristics of the
             algorithms used in product generation. These characteristics
             include the algorithm package name, date, version, maturity
             code and generating system characteristics for the package. -->
        <!-- No DIF mapping
        <AlgorithmPackage>
          <Name></Name>
          <Version></Version>
          <Description></Description>
        </AlgorithmPackage>
        End No Mapping -->

        <!-- ================== Begin SpatialInfo ======================= -->
        <!-- This entity stores the reference frame or system from which
             altitudes (elevations) are measured. The information contains
             the datum name, distance units and encoding method, which
             provide the definition for the system. This table also stores
             the characteristics of the reference frame or system from
             which depths are measured. The additional information in the
             table are geometry reference data etc. -->
        <!-- Data_Resolution/Vertical_Resolution in the DIF might be
             mapped to ECHO AltitudeResolutions/AltitudeResolutions -->
        <!-- No DIF mapping
        <SpatialInfo>
          <SpatialCoverageType/>
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
          <PlanarCoordinateSystems>
            <PlanarCoordinateSystem/>
          </PlanarCoordinateSystems>
          <DepthResolutions>
            <DepthResolution/>
          </DepthResolutions>
          <AltitudeResolutions>
            <AltitudeResolution/>
          </AltitudeResolutions>
        </SpatialInfo>
        End No Mapping -->
        <!-- ================== End SpatialInfo ========================= -->

        <!-- A null value is printed in the output if the Related_URL group
             does not exist in the DIF. If the Related_URL has a
             URL_Content_Type of 'GET DATA' it is mapped to an
             OnlineAccessURLs Group unless its Subtype is
             'OPENDAP DIRECTORY (DODS)'; all other URL_Content_Type
             are mapped to OnlineResources -->

        <!-- ================== Begin OnlineAccessURLs ================== -->
        <!-- Avoid including OPENDAP URLs, because they belong in
             AdditionalAttributes -->
        <!-- Changed in revision 1.20 to no longer create OnlineAccessURLs -->
        <!--
        <xsl:choose>
          <xsl:when test="Related_URL[(URL_Content_Type/Type = 'GET DATA') and ((not(URL_Content_Type/Subtype)) or (URL_Content_Type/Subtype != 'OPENDAP DIRECTORY (DODS)'))]">
            <OnlineAccessURLs>
              <xsl:apply-templates select="Related_URL[(URL_Content_Type/Type='GET DATA') and ((not(URL_Content_Type/Subtype)) or (URL_Content_Type/Subtype!='OPENDAP DIRECTORY (DODS)'))]" />
            </OnlineAccessURLs>
          </xsl:when>
        </xsl:choose>
        -->
        <!-- ================== End OnlineAccessURLs ==================== -->

        <!-- ================== Begin OnlineResources =================== -->
        <xsl:choose>
          <!-- Changed in revision 1.20 to make every Related_URL that is
               not an OPeNDAP URL into an OnlineResource -->
          <!--
          <xsl:when test='Related_URL[(URL_Content_Type/Type != "GET DATA")]'>
          -->
          <xsl:when test="Related_URL[(not(URL_Content_Type/Subtype)) or ((URL_Content_Type/Subtype != 'OPENDAP DIRECTORY (DODS)') and (URL_Content_Type/Subtype != 'OPENDAP DATA (DODS)'))]">
            <OnlineResources>
              <!--
              <xsl:apply-templates select='Related_URL[(URL_Content_Type/Type != "GET DATA")]' />
              -->
              <xsl:apply-templates select="Related_URL[(not(URL_Content_Type/Subtype)) or ((URL_Content_Type/Subtype != 'OPENDAP DIRECTORY (DODS)') and (URL_Content_Type/Subtype != 'OPENDAP DATA (DODS)'))]" />
            </OnlineResources>
          </xsl:when>
        </xsl:choose>
        <!-- ================== End OnlineResources ===================== -->

        <!-- ================== Begin AssociatedDIFs ==================== -->
        <AssociatedDIFs>
          <DIF>
            <EntryId>
              <xsl:apply-templates select="Entry_ID" />
            </EntryId>
          </DIF>
        </AssociatedDIFs>
        <!-- ================== End AssociatedDIFs ====================== -->

        <!-- ================== Begin Spatial =========================== -->
        <xsl:choose>
          <!-- Allow 4 mutually exclusive cases:
               1) Coverage=Orbit,      Representation=ORBIT
               2) Coverage=Vertical,   Representation=GEODETIC
               3) Coverage=Horizontal, Representation=CARTESIAN
               4) No spatial coverage, Representation=NO_SPATIAL -->
          <xsl:when test="(ShortName/text() = 'OMAERUV') or (ShortName/text() = 'OMBRO') or (ShortName/text() = 'OMCLDO2') or (ShortName/text() = 'OMCLDO2Z') or (ShortName/text() = 'OMCLDRR') or (ShortName/text() = 'OMDOAO3') or (ShortName/text() = 'OMDOAO3Z') or (ShortName/text() = 'OMHCHO') or (ShortName/text() = 'OMNO2') or (ShortName/text() = 'OMOCLO') or (ShortName/text() = 'OMSO2') or (ShortName/text() = 'OMTO3') or (ShortName/text() = 'OMAERO') or (ShortName/text() = 'OMAEROZ') or (ShortName/text() = 'OMPROO3') or (ShortName/text() = 'OMUVB') or (ShortName/text() = 'OML1BCAL') or (ShortName/text() = 'OML1BIRR') or (ShortName/text() = 'OML1BRUG') or (ShortName/text() = 'OML1BRUZ') or (ShortName/text() = 'OML1BRVG') or (ShortName/text() = 'OML1BRVZ') or (ShortName/text() = 'OMO3PR') or (ShortName/text() = 'OMPIXCOR') or (ShortName/text() = 'OMPIXCORZ')">
            <Spatial>
              <SpatialCoverageType>Orbit</SpatialCoverageType>
              <OrbitParameters>
                <SwathWidth>2600</SwathWidth>
                <Period>100</Period>
                <InclinationAngle>98.2</InclinationAngle>
                <NumberOfOrbits>1</NumberOfOrbits>
              </OrbitParameters>
              <GranuleSpatialRepresentation>ORBIT</GranuleSpatialRepresentation>
            </Spatial>
          </xsl:when>
          <xsl:when test="(ShortName/text() = 'SWDB_L2')">
            <Spatial>
              <SpatialCoverageType>Orbit</SpatialCoverageType>
              <OrbitParameters>
                <SwathWidth>1502</SwathWidth>
                <Period>99</Period>
                <InclinationAngle>98.333333</InclinationAngle>
                <NumberOfOrbits>1</NumberOfOrbits>
              </OrbitParameters>
              <GranuleSpatialRepresentation>ORBIT</GranuleSpatialRepresentation>
            </Spatial>
          </xsl:when>
          <xsl:when test="(ShortName/text() = 'AIRABRAD') or (ShortName/text() = 'AIRHBRAD') or (ShortName/text() = 'AIRIBQAP') or (ShortName/text() = 'AIRIBRAD') or (ShortName/text() = 'AIRVBQAP') or (ShortName/text() = 'AIRVBRAD') or (ShortName/text() = 'TOMSEPL2') or (ShortName/text() = 'TOMSN7L2') or (ShortName/text() = 'TOMSN7L1BRAD') or (ShortName/text() = 'OCO2_L1B_Science') or (ShortName/text() = 'OCO2_L2_IMAPDOAS') or (ShortName/text() = 'OCO2_L2_Diagnostic') or (ShortName/text() = 'OCO2_L2_Standard')">
            <Spatial>
              <SpatialCoverageType>Vertical</SpatialCoverageType>
              <VerticalSpatialDomain>
                <Type>Minimum Altitude</Type>
                <Value>SFC</Value>
              </VerticalSpatialDomain>
              <VerticalSpatialDomain>
                <Type>Maximum Altitude</Type>
                <Value>TOA</Value>
              </VerticalSpatialDomain>
              <GranuleSpatialRepresentation>GEODETIC</GranuleSpatialRepresentation>
            </Spatial>
          </xsl:when>
          <xsl:when test="Spatial_Coverage">
            <Spatial>
              <SpatialCoverageType>Horizontal</SpatialCoverageType>
              <HorizontalSpatialDomain>
                <!-- No DIF Mapping
                <ZoneIdentifier/>
                End No Mapping -->
                <Geometry>
                  <CoordinateSystem>CARTESIAN</CoordinateSystem>
                  <!-- *** No Mapping from DIF ***
                  <Point>
                    <PointLongitude/>
                    <PointLatitude/>
                  </Point>
                  <Circle>
                    <CenterPoint/>
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
                  <Line>
                    <Point/>
                  </Line>
                  -->
                </Geometry>
              </HorizontalSpatialDomain>

              <!-- No DIF Mapping
              <VerticalSpatialDomain>
                <Type/>
                <Value/>
              </VerticalSpatialDomain>
              End No Mapping -->
              <GranuleSpatialRepresentation>CARTESIAN</GranuleSpatialRepresentation>
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
                <!-- SpatialCoverageType is commented out because it does not
                     make sense to include it if there is no spatial coverage.
                <SpatialCoverageType>Horizontal</SpatialCoverageType>
                -->
              <GranuleSpatialRepresentation>NO_SPATIAL</GranuleSpatialRepresentation>
            </Spatial>
          </xsl:otherwise>
        </xsl:choose>

        <!-- Orbit Parameters, GranuleSpatialRepresentation, and
             GranuleSpatialInheritance were skipped. There is no mapping
             from DIF fields -->
        <!-- ================== End Spatial ============================= -->

        <!-- No DIF Mapping
        <AssociatedBrowseImages/>
        End No Mapping -->

      </Collection>
    </Collections>
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

<xsl:template match="Originating_Center">
  <!-- Handle GCMD bug that produces empty Originating_Center tag -->
  <xsl:if test="normalize-space(.)">
    <ProcessingCenter><xsl:value-of select="." /></ProcessingCenter>    
  </xsl:if>
</xsl:template>

<xsl:template match="Data_Set_Progress">
  <CollectionState><xsl:value-of select="." /></CollectionState>
</xsl:template>

<xsl:template match="Access_Constraints">
  <RestrictionComment><xsl:value-of select="normalize-space(.)" /></RestrictionComment>
</xsl:template>

<xsl:template match="Distribution/Distribution_Format">
  <DataFormat><xsl:value-of select="." /></DataFormat>
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
    <!--
    <xsl:when test="URL_Content_Type/Type/text() = 'GET DATA'">
      <xsl:choose>
    -->
        <xsl:when test="(URL_Content_Type/Subtype/text() = 'OPENDAP DIRECTORY (DODS)') or (URL_Content_Type/Subtype = 'OPENDAP DATA (DODS)')">
          <!-- DIF Related_URL GET DATA specifies an OPeNDAP server -->
          <AdditionalAttribute>
            <Name>OPeNDAPServer</Name>
            <DataType>STRING</DataType>
            <Description>
              <xsl:apply-templates select="Description" />
            </Description>
            <Value>
              <xsl:apply-templates select="URL" />
            </Value>
          </AdditionalAttribute>
        </xsl:when>
    <!--
        <xsl:otherwise>
    -->
          <!-- DIF Related_URL GET DATA is ECHO OnlineAccessUrl -->
    <!--
          <xsl:for-each select="URL">
            <OnlineAccessURL>
              <URL>
                <xsl:value-of select="." />
              </URL>
              <URLDescription>
                <xsl:apply-templates select="../Description[/DIF/Related_URL/URL_Content_Type/Type='GET DATA']" />
              </URLDescription>
    -->
              <!-- No DIF Mapping
              <MimeType/>
              End No Mapping -->
    <!--
            </OnlineAccessURL>
          </xsl:for-each>
        </xsl:otherwise>
      </xsl:choose>
    </xsl:when>
    -->
    <xsl:otherwise>
      <xsl:for-each select="URL">
        <OnlineResource>
          <URL>
            <xsl:value-of select="." />
          </URL>
          <Description>
            <xsl:apply-templates select="../Description" />
          </Description>
          <Type>
            <xsl:apply-templates select="../URL_Content_Type" />
          </Type>
          <!-- No DIF Mapping
          <MimeType/>
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
    <ShortName>
      <xsl:value-of select="Short_Name" />
    </ShortName>
    <LongName>
      <xsl:apply-templates select="Long_Name" />
    </LongName>
    <!-- No DIF Mapping
    <StartDate/>
    <EndDate/>
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
  <Keyword>
    <xsl:value-of select="." />
  </Keyword>
</xsl:template>

<xsl:template match="Location_Type">
  <Keyword>
    <xsl:value-of select="." />
  </Keyword>
</xsl:template>

<xsl:template match="Location_Subregion1">
  <Keyword>
    <xsl:value-of select="." />
  </Keyword>
</xsl:template>

<xsl:template match="Detailed_Location">
  <Keyword>
    <xsl:value-of select="." />
  </Keyword>
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
    <Instructions/>
    End No Mapping -->

    <!-- Inserts DIF Data Center Name into ECHO ContactOrganizationName
         for a contact whose role is Data Center Contact -->
    <xsl:choose>
      <xsl:when test="Personnel[Role='DATA CENTER CONTACT']">
        <OrganizationName>
          <xsl:apply-templates select="Data_Center_Name" />
        </OrganizationName>
      </xsl:when>
    </xsl:choose>

    <xsl:apply-templates select="Contact_Address" />
    <xsl:call-template name="PhoneFax">
      <xsl:with-param name="Phone" select="Phone" />
      <xsl:with-param name="Fax" select="Fax" />
    </xsl:call-template>
    <xsl:apply-templates select="Email" />
    <ContactPersons>
      <ContactPerson>
        <!-- added to look for missing First_Name -->
        <xsl:choose>
          <xsl:when test="./First_Name">
            <xsl:apply-templates select="First_Name" />
          </xsl:when>
          <xsl:otherwise>
            <FirstName>PLEASE CONTACT</FirstName>
          </xsl:otherwise>
        </xsl:choose>
        <!-- end of looking for missing First_Name -->

        <xsl:apply-templates select="Middle_Name" />
        <xsl:apply-templates select="Last_Name" />
        <!-- No DIF Mapping
        <JobPosition/>
        End No Mapping -->
      </ContactPerson>
    </ContactPersons>
  </Contact>
</xsl:template>

<xsl:template match="Role">
  <xsl:value-of select="." />
  <xsl:if test = "not(position()=last())" >
    <xsl:text>, </xsl:text>
  </xsl:if>
</xsl:template>

<xsl:template name="PhoneFax">
  <xsl:param name="Phone" />
  <xsl:param name="Fax" />
  <xsl:if test="$Phone or $Fax">
    <OrganizationPhones>
      <xsl:if test="$Phone">
        <Phone>
          <Number><xsl:value-of select="$Phone" /></Number>
          <Type>Direct Line</Type>
        </Phone>
      </xsl:if>
      <xsl:if test="$Fax">
        <Phone>
          <Number><xsl:value-of select="$Fax" /></Number>
          <Type>Fax</Type>
        </Phone>
      </xsl:if>
    </OrganizationPhones>
  </xsl:if>
</xsl:template>

<xsl:template match="Email">
  <OrganizationEmails>
    <Email>
      <xsl:value-of select="." />
    </Email>
  </OrganizationEmails>
</xsl:template>

<xsl:template match="First_Name">
  <FirstName>
    <xsl:value-of select="." />
  </FirstName>
</xsl:template>

<!-- Provide a default value for First_Name if value is missing -->
<xsl:template match="First_Name[not(text())]">
  <FirstName>PLEASE CONTACT</FirstName>
</xsl:template>

<xsl:template match="Middle_Name">
  <MiddleName>
    <xsl:value-of select="." />
  </MiddleName>
</xsl:template>

<xsl:template match="Last_Name">
  <LastName>
    <xsl:value-of select="." />
  </LastName>
</xsl:template>

<xsl:template match="Data_Center_Name">
  <xsl:value-of select="." />
</xsl:template>

<xsl:template match="Contact_Address">
  <!-- Do not include OrganizationAddress unless all fields are present -->
  <xsl:choose>
    <xsl:when test="./Address and ./City and ./Province_or_State and ./Postal_Code and ./Country">
      <OrganizationAddresses>
        <Address>
          <StreetAddress>
            <xsl:apply-templates select="Address" />
          </StreetAddress>
          <xsl:apply-templates select="City" />
          <xsl:apply-templates select="Province_or_State" />
          <xsl:apply-templates select="Postal_Code" />
          <xsl:apply-templates select="Country" />
        </Address>
      </OrganizationAddresses>
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
  <Sensor>
    <ShortName>
      <xsl:value-of select="Short_Name" />
    </ShortName>
    <!-- If there is no value for Long_Name, use the value of Short_Name
         for Long_Name -->
    <xsl:choose>
      <xsl:when test="Long_Name">
        <LongName><xsl:value-of select="Long_Name" /></LongName>
      </xsl:when>
      <xsl:otherwise>
        <LongName><xsl:value-of select="Short_Name" /></LongName>
      </xsl:otherwise>
    </xsl:choose>
    <!-- No DIF Mapping
    <Technique/>
    <Characteristics>
      <Characteristic>
        <Name/>
        <Description/>
        <DataType/>
        <Unit/>
        <Value/>
      </Characteristic>
    </Characteristics>
    End No Mapping -->
  </Sensor>
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
with an instrument and sensor obtained from the first sensor encountered
in the DIF.
If the Short_Name is 'MODELS', then the Platform section is not relevant,
so don't include it.
-->
<xsl:template match="Source_Name">
  <xsl:if test = "(position()=1) and (Short_Name/text() != 'MODELS')" >
  <Platforms>
    <Platform>
      <ShortName><xsl:value-of select="Short_Name" /></ShortName>
      <!-- If there is no value for Long_Name, use the value of Short_Name for
           Long_Name -->
      <xsl:choose>
        <xsl:when test="Long_Name">
          <LongName><xsl:value-of select="Long_Name" /></LongName>
        </xsl:when>
        <xsl:otherwise>
          <LongName><xsl:value-of select="Short_Name" /></LongName>
        </xsl:otherwise>
      </xsl:choose>
      <Type>Not Specified</Type>
      <!-- No DIF Mapping
      <Characteristics>
        <Characteristic>
          <Name/>
          <Description/>
          <DataType/>
          <Unit/>
          <Value/>
        </Characteristic>
      </Characteristics>
      End No Mapping -->
      <xsl:if test = "(//Sensor_Name/Short_Name)" >
      <Instruments>
        <Instrument>
          <ShortName><xsl:value-of select="//Sensor_Name/Short_Name" /></ShortName>
          <!-- If there is no value for Long_Name, use the value of Short_Name
               for Long_Name -->
          <xsl:choose>
            <xsl:when test="//Sensor_Name/Long_Name">
              <LongName><xsl:value-of select="//Sensor_Name/Long_Name" /></LongName>
            </xsl:when>
            <xsl:otherwise>
              <LongName><xsl:value-of select="//Sensor_Name/Short_Name" /></LongName>
            </xsl:otherwise>
          </xsl:choose>
          <!-- No DIF Mapping
          <Technique/>
          <NumberOfSensors/>
          <Characteristics>
            <Characteristic>
              <Name/>
              <Description/>
              <DataType/>
              <Unit/>
              <Value/>
            </Characteristic>
          </Characteristics>
          End No Mapping -->
          <Sensors>
            <Sensor>
              <ShortName><xsl:value-of select="//Sensor_Name/Short_Name" /></ShortName>
              <xsl:choose>
                <xsl:when test="//Sensor_Name/Long_Name">
                  <LongName><xsl:value-of select="//Sensor_Name/Long_Name" /></LongName>
                </xsl:when>
                <xsl:otherwise>
                  <LongName><xsl:value-of select="//Sensor_Name/Short_Name" /></LongName>
                </xsl:otherwise>
              </xsl:choose>
              <!-- No DIF Mapping
              <Technique/>
              <Characteristics>
                <Characteristic>
                  <Name/>
                  <Description/>
                  <DataType/>
                  <Unit/>
                  <Value/>
                </Characteristic>
              </Characteristics>
              End No Mapping -->
            </Sensor>
          </Sensors>
          <!-- No DIF Mapping
          <OperationModes/>
          End No Mapping -->
        </Instrument>
      </Instruments>
      </xsl:if>
    </Platform>
  </Platforms>
  </xsl:if>
</xsl:template>
<!--
If we wanted all platforms to be populated with all sensors, this is how we
would do it.
<xsl:template match="Source_Name">
  <Platforms>
    <Platform>
      <ShortName><xsl:value-of select="Short_Name" /></ShortName>
      <xsl:choose>
        <xsl:when test="Long_Name">
          <LongName><xsl:value-of select="Long_Name" /></LongName>
        </xsl:when>
        <xsl:otherwise>
          <LongName><xsl:value-of select="Short_Name" /></LongName>
        </xsl:otherwise>
      </xsl:choose>
      <Type>Not Specified</Type>
-->
      <!-- No DIF Mapping
      <Characteristics>
        <Characteristic>
          <Name/>
          <Description/>
          <DataType/>
          <Unit/>
          <Value/>
        </Characteristic>
      </Characteristics>
      End No Mapping -->
<!--
      <Instruments>
        <xsl:for-each select="//Sensor_Name">
          <Instrument>
            <ShortName><xsl:value-of select="./Short_Name" /></ShortName>
            <xsl:choose>
              <xsl:when test="./Long_Name">
                <LongName><xsl:value-of select="./Long_Name" /></LongName>
              </xsl:when>
              <xsl:otherwise>
                <LongName><xsl:value-of select="./Short_Name" /></LongName>
              </xsl:otherwise>
            </xsl:choose>
-->
            <!-- No DIF Mapping
            <Technique/>
            <NumberOfSensors/>
            <Characteristics>
              <Characteristic>
                <Name/>
                <Description/>
                <DataType/>
                <Unit/>
                <Value/>
              </Characteristic>
            </Characteristics>
            End No Mapping -->
<!--
            <Sensors>
              <Sensor>
                <ShortName><xsl:value-of select="./Short_Name" /></ShortName>
                <LongName><xsl:value-of select="./Long_Name" /></LongName>
-->
                <!-- No DIF Mapping
                <Technique/>
                <Characteristics>
                  <Characteristic>
                    <Name/>
                    <Description/>
                    <DataType/>
                    <Unit/>
                    <Value/>
                  </Characteristic>
                </Characteristics>
                End No Mapping -->
<!--
              </Sensor>
            <Sensors>
          </Instrument>
        </xsl:for-each>
      </Instruments>
    </Platform>
  </Platforms>
</xsl:template>
-->

<xsl:template match="Start_Date">
  <BeginningDateTime>
    <xsl:value-of select="." /><xsl:text>T00:00:00Z</xsl:text>
  </BeginningDateTime>
</xsl:template>

<xsl:template match="Stop_Date">
  <EndingDateTime>
    <xsl:value-of select="." /><xsl:text>T23:59:59.999999Z</xsl:text>
  </EndingDateTime>
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
