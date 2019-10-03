<?xml version="1.0" encoding="utf-8"?>
<!-- $Id: S4paGran2ECHO10.xsl,v 1.27 2015/06/25 19:42:39 eseiler Exp $ -->
<!-- -@@@ S4PA, Version $Name:  $ -->
<xsl:stylesheet
xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">
  <xsl:output method="xml" version="1.0" encoding="UTF-8" indent="yes"/>

  <!-- This is an XSLT style sheet that transforms S4PA granule metadata
       to ECHO Granule metadata, using the ECHO 10 schema.
  -->
  <xsl:template match="/">
    <xsl:for-each select="S4PAGranuleMetaDataFile">
      <GranuleMetaDataFile xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="http://www.echo.nasa.gov/ingest/schemas/operations/Granule.xsd">
        <Granules>
          <Granule>
            <GranuleUR><xsl:value-of select="CollectionMetaData/ShortName"/>.<xsl:value-of select="CollectionMetaData/VersionID"/>:<xsl:value-of select="DataGranule/GranuleID"/></GranuleUR>

            <!-- The date/time this granule was entered into the data
                 provider's database -->
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

            <!-- The date/time that the data provider deleted the granule
                 from the data provider's database -->
            <xsl:if test="contains(CollectionMetaData/ShortName,'NRT')">
              <DeleteTime>
                <xsl:call-template name="AddDays">
                  <xsl:with-param name="DateTime" select="DataGranule/InsertDateTime"/>
                  <xsl:with-param name="NumDays">14</xsl:with-param>
                </xsl:call-template>
              </DeleteTime>
            </xsl:if>

            <!-- Reference from the granule to the collection it belongs to -->
            <Collection>
              <ShortName><xsl:value-of select="CollectionMetaData/ShortName"/></ShortName>
              <VersionId><xsl:value-of select="CollectionMetaData/VersionID"/></VersionId>
            </Collection>

            <!-- Numerical value indicating the type of restriction
                 applied to the granule for data access -->
            <!-- <RestrictionFlag>0</RestrictionFlag> -->

            <!-- <RestrictionComment></RestrictionComment> -->

            <DataGranule>
              <SizeMBDataGranule><xsl:value-of select="DataGranule/SizeBytesDataGranule div 1048576.0" /></SizeMBDataGranule>
              <ProducerGranuleId><xsl:value-of select="DataGranule/GranuleID"/></ProducerGranuleId>
              <xsl:choose>
                <xsl:when test="DataGranule/DayNightFlag">
                  <DayNightFlag>
                    <xsl:value-of select="translate(DataGranule/DayNightFlag,'abcdefghijklmnopqrstuvwxyz','ABCDEFGHIJKLMNOPQRSTUVWXYZ')"/>
                  </DayNightFlag>
                </xsl:when>
                <xsl:otherwise>
                  <DayNightFlag>UNSPECIFIED</DayNightFlag>
                </xsl:otherwise>
              </xsl:choose>
              <xsl:choose>
                <xsl:when test="DataGranule/ProductionDateTime/text()">
                  <xsl:choose>
                    <xsl:when test="substring(DataGranule/ProductionDateTime,11,1) = ' '">
                      <!-- If there is a space between date and time, e.g.
                           2007-04-12 23:37:57.000 then convert to the format
                           2007-04-12T23:37:57.000Z -->
                      <ProductionDateTime><xsl:value-of select="substring-before(DataGranule/ProductionDateTime, ' ')" /><xsl:text>T</xsl:text><xsl:value-of select="substring-after(DataGranule/ProductionDateTime, ' ')" /><xsl:text>Z</xsl:text></ProductionDateTime>
                    </xsl:when>
                    <!-- Usually the timezone for ProductionDateTime is 'Z',
                         but sometimes it is not (e.g. for some OMI L3 data
                         sets). Handle the cases of EST and EDT by converting
                         them to offset values. -->
                    <xsl:when test="substring-before(DataGranule/ProductionDateTime,'EST')">
                      <ProductionDateTime><xsl:value-of select="substring-before(DataGranule/ProductionDateTime,'EST')" /><xsl:text>-05:00</xsl:text></ProductionDateTime>
                    </xsl:when>
                    <xsl:when test="substring-before(DataGranule/ProductionDateTime,'EDT')">
                      <ProductionDateTime><xsl:value-of select="substring-before(DataGranule/ProductionDateTime,'EDT')" /><xsl:text>-04:00</xsl:text></ProductionDateTime>
                    </xsl:when>
                    <xsl:otherwise>
                      <xsl:call-template name="copy">
                        <xsl:with-param name="nodeList" select="DataGranule/ProductionDateTime" />
                      </xsl:call-template>
                    </xsl:otherwise>
                  </xsl:choose>
                </xsl:when>
                <xsl:otherwise>
                  <!-- We must provide a ProductionDateTime, so if there is
                       no value in the granule meatdata, assume there will
                       be a value for InsertDateTime, and use that as
                       the (approximate) ProductionDateTime) -->
                  <xsl:choose>
                    <xsl:when test="substring(DataGranule/InsertDateTime,11,1) = ' '">
                      <!-- If there is a space between date and time, e.g.
                           2007-04-12 23:37:57.000 then convert to the format
                           2007-04-12T23:37:57.000Z -->
                      <ProductionDateTime><xsl:value-of select="substring-before(DataGranule/InsertDateTime, ' ')" /><xsl:text>T</xsl:text><xsl:value-of select="substring-after(DataGranule/InsertDateTime, ' ')" /><xsl:text>Z</xsl:text></ProductionDateTime>
                    </xsl:when>
                    <xsl:otherwise>
                      <ProductionDateTime><xsl:value-of select="DataGranule/InsertDateTime"/></ProductionDateTime>
                    </xsl:otherwise>
                  </xsl:choose>
                </xsl:otherwise>
              </xsl:choose>
            </DataGranule>

           <xsl:choose>
             <!-- If PGEVersion is longer than 10 characters ... -->
             <xsl:when test="string-length(DataGranule/PGEVersionClass/PGEVersion) > 10">
               <PGEVersionClass>
               <xsl:choose>
                 <!-- ... reformat if PGEVersion contains '-NRT' -->
                 <xsl:when test="contains(DataGranule/PGEVersionClass/PGEVersion,'-NRT')">
                   <PGEVersion><xsl:value-of select="substring-before(DataGranule/PGEVersionClass/PGEVersion, '-NRT')"/><xsl:value-of select="substring-after(DataGranule/PGEVersionClass/PGEVersion, '-NRT')"/><xsl:text>N</xsl:text></PGEVersion>
                 </xsl:when>
                 <!-- ... otherwise truncate to first 10 characters -->
                 <xsl:otherwise>
                   <PGEVersion><xsl:value-of select="substring(DataGranule/PGEVersionClass/PGEVersion,1,10)"/></PGEVersion>
                 </xsl:otherwise>
               </xsl:choose>
               </PGEVersionClass>
             </xsl:when>
             <!-- If PGEVersion length is properly sized, capture entire object -->
             <xsl:otherwise>
               <xsl:call-template name="copy">
                 <xsl:with-param name="nodeList" select="DataGranule/PGEVersionClass" />
               </xsl:call-template>
             </xsl:otherwise>
           </xsl:choose>

            <Temporal>
              <RangeDateTime>
                <BeginningDateTime>
                  <xsl:value-of select="RangeDateTime/RangeBeginningDate"/>
                  <xsl:text>T</xsl:text>
                  <xsl:value-of select="RangeDateTime/RangeBeginningTime"/>
                  <xsl:if test="substring(RangeDateTime/RangeBeginningTime,string-length(RangeDateTime/RangeBeginningTime),1) != 'Z'">
                    <xsl:text>Z</xsl:text>
                  </xsl:if>
                </BeginningDateTime>
                <EndingDateTime>
                  <xsl:value-of select="RangeDateTime/RangeEndingDate"/>
                  <xsl:text>T</xsl:text>
                  <xsl:value-of select="RangeDateTime/RangeEndingTime"/>
                  <xsl:if test="substring(RangeDateTime/RangeEndingTime,string-length(RangeDateTime/RangeEndingTime),1) != 'Z'">
                    <xsl:text>Z</xsl:text>
                  </xsl:if>
                </EndingDateTime>
              </RangeDateTime>
            </Temporal>

            <!-- ================== Begin Spatial ========================= -->
            <xsl:choose>
              <!-- The only known datasets which require an
                   OrbitCalculatedSpatialDomain are OMI L1B and L2 orbital
                   swath datasets, which can be identified by the ShortName
                   in the CollectionMetaData.
                   -->
              <xsl:when test="(CollectionMetaData/ShortName/text() = 'OMAERUV') or (CollectionMetaData/ShortName/text() = 'OMBRO') or (CollectionMetaData/ShortName/text() = 'OMCLDO2') or (CollectionMetaData/ShortName/text() = 'OMCLDO2Z') or (CollectionMetaData/ShortName/text() = 'OMCLDRR') or (CollectionMetaData/ShortName/text() = 'OMDOAO3') or (CollectionMetaData/ShortName/text() = 'OMDOAO3Z') or (CollectionMetaData/ShortName/text() = 'OMHCHO') or (CollectionMetaData/ShortName/text() = 'OMNO2') or (CollectionMetaData/ShortName/text() = 'OMOCLO') or (CollectionMetaData/ShortName/text() = 'OMSO2') or (CollectionMetaData/ShortName/text() = 'OMTO3') or (CollectionMetaData/ShortName/text() = 'OMAERO') or (CollectionMetaData/ShortName/text() = 'OMAEROZ') or (CollectionMetaData/ShortName/text() = 'OMPROO3') or (CollectionMetaData/ShortName/text() = 'OMUVB') or (CollectionMetaData/ShortName/text() = 'OML1BCAL') or (CollectionMetaData/ShortName/text() = 'OML1BIRR') or (CollectionMetaData/ShortName/text() = 'OML1BRUG') or (CollectionMetaData/ShortName/text() = 'OML1BRUZ') or (CollectionMetaData/ShortName/text() = 'OML1BRVG') or (CollectionMetaData/ShortName/text() = 'OML1BRVZ') or (CollectionMetaData/ShortName/text() = 'OMO3PR') or (CollectionMetaData/ShortName/text() = 'OMPIXCOR') or (CollectionMetaData/ShortName/text() = 'OMPIXCORZ')">
                <Spatial>
                  <HorizontalSpatialDomain>
                    <ZoneIdentifier>Text</ZoneIdentifier>
                    <xsl:apply-templates select="OrbitCalculatedSpatialDomain/OrbitCalculatedSpatialDomainContainer/Orbit" />
                  </HorizontalSpatialDomain>
                </Spatial>
              </xsl:when>

              <xsl:when test="(CollectionMetaData/ShortName/text() = 'SWDB_L2')">
                <Spatial>
                  <HorizontalSpatialDomain>
                    <ZoneIdentifier>Text</ZoneIdentifier>
                    <Orbit>
                      <AscendingCrossing>
                        <xsl:choose>
                          <xsl:when test="(OrbitCalculatedSpatialDomain/OrbitCalculatedSpatialDomainContainer/Orbit/DescendingCrossing &gt; -12.5)">
                            <xsl:value-of select="OrbitCalculatedSpatialDomain/OrbitCalculatedSpatialDomainContainer/Orbit/DescendingCrossing - 167.5" />
                          </xsl:when>
                          <xsl:otherwise>
                            <xsl:value-of select="OrbitCalculatedSpatialDomain/OrbitCalculatedSpatialDomainContainer/Orbit/DescendingCrossing + 192.5" />
                          </xsl:otherwise>
                        </xsl:choose>
                      </AscendingCrossing>
                      <xsl:call-template name="copy">
                        <xsl:with-param name="nodeList" select="OrbitCalculatedSpatialDomain/OrbitCalculatedSpatialDomainContainer/Orbit/StartLat" />
                      </xsl:call-template>
                      <xsl:call-template name="copy">
                        <xsl:with-param name="nodeList" select="OrbitCalculatedSpatialDomain/OrbitCalculatedSpatialDomainContainer/Orbit/StartDirection" />
                      </xsl:call-template>
                      <xsl:call-template name="copy">
                        <xsl:with-param name="nodeList" select="OrbitCalculatedSpatialDomain/OrbitCalculatedSpatialDomainContainer/Orbit/EndLat" />
                      </xsl:call-template>
                      <xsl:call-template name="copy">
                        <xsl:with-param name="nodeList" select="OrbitCalculatedSpatialDomain/OrbitCalculatedSpatialDomainContainer/Orbit/EndDirection" />
                      </xsl:call-template>
                      <xsl:call-template name="copy">
                        <xsl:with-param name="nodeList" select="OrbitCalculatedSpatialDomain/OrbitCalculatedSpatialDomainContainer/Orbit/CenterPoint" />
                      </xsl:call-template>
                    </Orbit>
                  </HorizontalSpatialDomain>
                </Spatial>
              </xsl:when>

              <!-- Special case: MLS L1 and SORCE granules do not have a
                   Spatial, so create one -->
              <xsl:when test="(CollectionMetaData/ShortName/text() = 'ML1OA') or (CollectionMetaData/ShortName/text() = 'ML1RADD')">
                <Spatial>
                  <HorizontalSpatialDomain>
                    <Geometry>
                      <BoundingRectangle>
                        <WestBoundingCoordinate>-180</WestBoundingCoordinate>
                        <NorthBoundingCoordinate>82</NorthBoundingCoordinate>
                        <EastBoundingCoordinate>180</EastBoundingCoordinate>
                        <SouthBoundingCoordinate>-82</SouthBoundingCoordinate>
                      </BoundingRectangle>
                    </Geometry>
                  </HorizontalSpatialDomain>
                </Spatial>
              </xsl:when>

              <xsl:when test="(CollectionMetaData/ShortName/text() = 'ML1RADG') or (CollectionMetaData/ShortName/text() = 'ML1RADT') or (CollectionMetaData/ShortName/text() = 'SOR3TSI6') or (CollectionMetaData/ShortName/text() = 'SOR3TSID') or (CollectionMetaData/ShortName/text() = 'SOR3SSID') or (CollectionMetaData/ShortName/text() = 'UARSO3BS') or (CollectionMetaData/ShortName/text() = 'UARSU3BS') or (CollectionMetaData/ShortName/text() = 'SOR3SIMD') or (CollectionMetaData/ShortName/text() = 'SOR3SOLFUVD') or (CollectionMetaData/ShortName/text() = 'SOR3SOLMUVD') or (CollectionMetaData/ShortName/text() = 'SOR3XPS6') or (CollectionMetaData/ShortName/text() = 'SOR3XPSD') or (CollectionMetaData/ShortName/text() = 'AIRG2SSD') or (CollectionMetaData/ShortName/text() = 'TCTE3TSI6') or (CollectionMetaData/ShortName/text() = 'TCTE3TSID') or (CollectionMetaData/ShortName/text() = 'OCO2_Att') or (CollectionMetaData/ShortName/text() = 'OCO2_Eph') or (CollectionMetaData/ShortName/text() = 'OCO2_L1aIn_Pixel') or (CollectionMetaData/ShortName/text() = 'OCO2_L1aIn_Sample') or (CollectionMetaData/ShortName/text() = 'OCO2_L1B_Calibration') ">
                <Spatial>
                  <HorizontalSpatialDomain>
                    <Geometry>
                      <BoundingRectangle>
                        <WestBoundingCoordinate>-180</WestBoundingCoordinate>
                        <NorthBoundingCoordinate>90</NorthBoundingCoordinate>
                        <EastBoundingCoordinate>180</EastBoundingCoordinate>
                        <SouthBoundingCoordinate>-90</SouthBoundingCoordinate>
                      </BoundingRectangle>
                    </Geometry>
                  </HorizontalSpatialDomain>
                </Spatial>
              </xsl:when>

              <xsl:otherwise>
                <!-- All other datasets that have a SpatialDomainContainer
                     in the xml being transformed will include a
                     Spatial in the transformed output.
                     -->
                <xsl:apply-templates select="SpatialDomainContainer" />
              </xsl:otherwise>
            </xsl:choose>
            <!-- ================== End Spatial =========================== -->

            <xsl:apply-templates select="OrbitCalculatedSpatialDomain" />

            <xsl:if test="MeasuredParameters">
              <MeasuredParameters>
                <xsl:apply-templates select="MeasuredParameters/MeasuredParameter" />
              </MeasuredParameters>
            </xsl:if>

            <!-- Commenting out Platform because it is not required
                 by the ECHO schema, and because there is not necessarily
                 agreement between the values found in the granule metadata
                 and the values in the collection metadata (determined by
                 the DIF).
            <xsl:if test="Platform">
              <Platforms>
                <xsl:apply-templates select="Platform" />
              </Platforms>
            </xsl:if>
            -->

            <!--
            <Campaigns></Campaigns>
            <AdditionalAttributes></AdditionalAttributes>
            <InputGranules></InputGranules>
            <TwoDCoordinateSystem></TwoDCoordinateSystem>
            <Price></Price>
            -->

            <!-- s4pa_publish_echo.pl will replace OnlineAccessURLs
                 with a value -->
            <OnlineAccessURLs/>

            <OnlineResources/>

            <Orderable>false</Orderable>
            <xsl:apply-templates select="DataGranule/Format" />
            <!--
            <Visible></Visible>
            <CloudCover></CloudCover>
            -->
            <xsl:if test="DataGranule/BrowseFile">
              <AssociatedBrowseImages>
                <xsl:apply-templates select="DataGranule/BrowseFile" />
              </AssociatedBrowseImages>
            </xsl:if>

          </Granule>
        </Granules>
      </GranuleMetaDataFile>
    </xsl:for-each>
  </xsl:template>

  <xsl:template name="FormatDateTime">
    <xsl:param name="DateTime"/>
    <xsl:choose>
      <xsl:when test="substring($DateTime,11,1) = ' '">
    <!-- If there is a space between date and time, e.g.
    2007-04-12 23:37:57.000 then convert to the format
    2007-04-12T23:37:57.000Z -->
        <xsl:value-of select="substring-before($DateTime, ' ')" />
        <xsl:text>T</xsl:text>
        <xsl:value-of select="substring-after($DateTime, ' ')" />
        <xsl:text>Z</xsl:text>
      </xsl:when>
      <xsl:otherwise>
        <xsl:value-of select="$DateTime"/>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template>

  <xsl:template name="AddDays">
    <xsl:param name="DateTime"/>
    <xsl:param name="NumDays"/>

    <xsl:variable name="dt">
      <xsl:call-template name="FormatDateTime">
        <xsl:with-param name="DateTime" select="$DateTime"/>
      </xsl:call-template>
    </xsl:variable>

    <xsl:variable name="JD">
      <xsl:call-template name="JDatefromCDate">
        <xsl:with-param name="cdate" select="substring-before($dt,'T')"/>
      </xsl:call-template>
    </xsl:variable>

    <xsl:variable name="modJD" select="$JD + $NumDays"/>

    <xsl:variable name="CD">
      <xsl:call-template name="CDatefromJDate">
        <xsl:with-param name="jdate" select="$modJD"/>
      </xsl:call-template>
    </xsl:variable>

    <xsl:value-of select="$CD"/>
    <xsl:text>T</xsl:text>
    <xsl:value-of select="substring-after($dt,'T')"/>
  </xsl:template>

  <xsl:template name="SubtractDays">
    <xsl:param name="DateTime"/>
    <xsl:param name="NumDays"/>

    <xsl:variable name="dt">
      <xsl:call-template name="FormatDateTime">
        <xsl:with-param name="DateTime" select="$DateTime"/>
      </xsl:call-template>
    </xsl:variable>

    <xsl:variable name="JD">
      <xsl:call-template name="JDatefromCDate">
        <xsl:with-param name="cdate" select="substring-before($dt,'T')"/>
      </xsl:call-template>
    </xsl:variable>

    <xsl:variable name="modJD" select="$JD - $NumDays"/>

    <xsl:variable name="CD">
      <xsl:call-template name="CDatefromJDate">
        <xsl:with-param name="jdate" select="$modJD"/>
      </xsl:call-template>
    </xsl:variable>

    <xsl:value-of select="$CD"/>
    <xsl:text>T</xsl:text>
    <xsl:value-of select="substring-after($dt,'T')"/>
  </xsl:template>

  <xsl:template name="JDatefromCDate">
    <xsl:param name="cdate"/>

    <xsl:variable name="year" select="substring($cdate,1,4)"/>
    <xsl:variable name="month" select="substring($cdate,6,2)"/>
    <xsl:variable name="day" select="substring($cdate,9,2)"/>

    <xsl:variable name="a" select="floor((14 - $month) div 12)"/>
    <xsl:variable name="y" select="$year + 4800 - $a"/>
    <xsl:variable name="m" select="$month + 12 * $a - 3"/>

    <xsl:value-of select="$day + floor((153 * $m + 2) div 5) + $y * 365 + floor($y div 4) - floor($y div 100) + floor($y div 400) - 32045"/>
  </xsl:template>

  <xsl:template name="CDatefromJDate">
    <xsl:param name="jdate"/>

    <xsl:variable name="a" select="$jdate + 32044"/>
    <xsl:variable name="b" select="floor((4 * $a + 3) div 146097)"/>
    <xsl:variable name="c" select="$a - 146097 * floor($b div 4)"/>

    <xsl:variable name="d" select="floor((4 * $c + 3) div 1461)"/>
    <xsl:variable name="e" select="$c - floor((1461 * $d) div 4)"/>
    <xsl:variable name="m" select="floor((5 * $e + 2) div 153)"/>

    <xsl:variable name="day" select="format-number($e - floor((153 * $m + 2) div 5) + 1,'00')"/>
    <xsl:variable name="month" select="format-number($m + 3 - (12 * floor($m div 10)),'00')"/>
    <xsl:variable name="year" select="format-number(100 * $b + $d - 4800 + floor($m div 10),'0000')"/>

    <xsl:value-of select="concat($year,'-',$month,'-',$day)"/>
  </xsl:template>

  <xsl:template match="SpatialDomainContainer">
    <xsl:choose>
      <!--
           Only create a Spatial if we have a
           HorizontalSpatialDomainContainer containing a
           GPolygon or a BoundingRectangle
      -->
      <xsl:when test="(./HorizontalSpatialDomainContainer/GPolygon) or (./HorizontalSpatialDomainContainer/BoundingRectangle)">
        <xsl:choose>
          <xsl:when test="(./HorizontalSpatialDomainContainer/BoundingRectangle/SouthBoundingCoordinate = 0) and (./HorizontalSpatialDomainContainer/BoundingRectangle/NorthBoundingCoordinate = 0) and (./HorizontalSpatialDomainContainer/BoundingRectangle/WestBoundingCoordinate = 0) and (./HorizontalSpatialDomainContainer/BoundingRectangle/EastBoundingCoordinate = 0)">
            <!-- Do not create a SpatialDomain in this case -->
          </xsl:when>
          <xsl:otherwise>
            <Spatial>
              <xsl:apply-templates select="LocalityValue" />
              <xsl:apply-templates select="VerticalSpatialDomain" />
              <HorizontalSpatialDomain>
                <xsl:call-template name="copy">
                  <xsl:with-param name="nodeList" select="./ZoneIdentifier" />
                </xsl:call-template>
                <xsl:choose>
                  <xsl:when test="./HorizontalSpatialDomainContainer/GPolygon">
                    <Geometry>
                      <xsl:call-template name="copyGPolygons">
                        <xsl:with-param name="nodeList" select="./HorizontalSpatialDomainContainer/GPolygon" />
                      </xsl:call-template>
                    </Geometry>
                  </xsl:when>
                  <xsl:otherwise>
                    <Geometry>
                      <xsl:apply-templates select="./HorizontalSpatialDomainContainer/BoundingRectangle" />
                    </Geometry>
                  </xsl:otherwise>
                </xsl:choose>
              </HorizontalSpatialDomain>
            </Spatial>
          </xsl:otherwise>
        </xsl:choose>
      </xsl:when>
    </xsl:choose>
  </xsl:template>

  <xsl:template match="OrbitCalculatedSpatialDomain">
    <OrbitCalculatedSpatialDomains>
      <xsl:apply-templates select="OrbitCalculatedSpatialDomainContainer" />
    </OrbitCalculatedSpatialDomains>
  </xsl:template>

  <xsl:template match="OrbitCalculatedSpatialDomainContainer">
    <OrbitCalculatedSpatialDomain>
      <xsl:call-template name="copy">
        <xsl:with-param name="nodeList" select="OrbitalModelName" />
      </xsl:call-template>
      <xsl:apply-templates select="OrbitNumber" />
      <xsl:call-template name="copy">
        <xsl:with-param name="nodeList" select="StartOrbitNumber" />
      </xsl:call-template>
      <xsl:call-template name="copy">
        <xsl:with-param name="nodeList" select="StopOrbitNumber" />
      </xsl:call-template>
      <xsl:call-template name="copy">
        <xsl:with-param name="nodeList" select="EquatorCrossingLongitude" />
      </xsl:call-template>
      <xsl:if test="EquatorCrossingDate">
        <EquatorCrossingDateTime>
          <xsl:value-of select="EquatorCrossingDate" />
          <xsl:text>T</xsl:text>
          <xsl:choose>
            <xsl:when test="EquatorCrossingTime">
              <xsl:value-of select="EquatorCrossingTime" />
            </xsl:when>
            <xsl:otherwise>
              <xsl:text>00:00:00</xsl:text>
            </xsl:otherwise>
          </xsl:choose>
          <xsl:text>Z</xsl:text>
        </EquatorCrossingDateTime>
      </xsl:if>
    </OrbitCalculatedSpatialDomain>
  </xsl:template>

  <xsl:template match="Orbit">
    <Orbit>
      <xsl:call-template name="copy">
        <xsl:with-param name="nodeList" select="AscendingCrossing" />
      </xsl:call-template>
      <xsl:call-template name="copy">
        <xsl:with-param name="nodeList" select="StartLat" />
      </xsl:call-template>
      <xsl:call-template name="copy">
        <xsl:with-param name="nodeList" select="StartDirection" />
      </xsl:call-template>
      <xsl:call-template name="copy">
        <xsl:with-param name="nodeList" select="EndLat" />
      </xsl:call-template>
      <xsl:call-template name="copy">
        <xsl:with-param name="nodeList" select="EndDirection" />
      </xsl:call-template>
      <xsl:call-template name="copy">
        <xsl:with-param name="nodeList" select="CenterPoint" />
      </xsl:call-template>
    </Orbit>
  </xsl:template>

  <xsl:template match="LocalityValue">
    <GranuleLocality>
      <xsl:copy-of select="." />
    </GranuleLocality>
  </xsl:template>

  <xsl:template match="VerticalSpatialDomain">
    <VerticalSpatialDomains>
      <xsl:apply-templates select="VerticalSpatialDomainContainer" />
    </VerticalSpatialDomains>
  </xsl:template>

  <xsl:template match="VerticalSpatialDomainContainer">
    <VerticalSpatialDomain>
      <Type><xsl:value-of select="VerticalSpatialDomainType"/></Type>
      <Value><xsl:value-of select="VerticalSpatialDomainValue"/></Value>
    </VerticalSpatialDomain>
  </xsl:template>

  <xsl:template match="BoundingRectangle">
    <!--
      <xsl:when test="(./SouthBoundingCoordinate = -90.0) and (./NorthBoundingCoordinate = 90.0) and (./WestBoundingCoordinate = -180.0) and (./EastBoundingCoordinate = 180.0)">
        <Global />
      </xsl:when>
    -->
    <xsl:choose>
      <!-- Expect certain (AIRS) ShortName values to correspond to a collection
           which has a GranuleSpatialRepresentation value of 'Geodetic',
           which allows either a BoundingRectangle or a GPolygon to be
           used in the HorizontalSpatialDomainContainer.
      -->
      <xsl:when test="(//CollectionMetaData/ShortName/text() = 'AIRABRAD') or (//CollectionMetaData/ShortName/text() = 'AIRHBRAD') or (//CollectionMetaData/ShortName/text() = 'AIRIBQAP') or (//CollectionMetaData/ShortName/text() = 'AIRIBRAD') or (//CollectionMetaData/ShortName/text() = 'AIRVBQAP') or (//CollectionMetaData/ShortName/text() = 'AIRVBRAD')">
        <xsl:choose>
          <xsl:when test="((./SouthBoundingCoordinate &lt; -89.999) or (((./WestBoundingCoordinate &lt; -180.0) or (./EastBoundingCoordinate &gt; 180.0)) and (./NorthBoundingCoordinate &lt; 0.0)) or ((./WestBoundingCoordinate = -180.0) and (./EastBoundingCoordinate = 180.0) and (./NorthBoundingCoordinate &lt; 0.0)))">
            <!-- When a "too south" value is seen,
                 or a "too west" or "too east" value is seen in the
                 southern hemisphere,
                 form a clockwise ring around the south pole -->
            <GPolygon>
              <Boundary>
                <Point>
                  <PointLongitude>0</PointLongitude>
                  <PointLatitude><xsl:value-of select="./NorthBoundingCoordinate" /></PointLatitude>
                </Point>
                <Point>
                  <PointLongitude>45</PointLongitude>
                  <PointLatitude><xsl:value-of select="./NorthBoundingCoordinate" /></PointLatitude>
                </Point>
                <Point>
                  <PointLongitude>90</PointLongitude>
                  <PointLatitude><xsl:value-of select="./NorthBoundingCoordinate" /></PointLatitude>
                </Point>
                <Point>
                  <PointLongitude>135</PointLongitude>
                  <PointLatitude><xsl:value-of select="./NorthBoundingCoordinate" /></PointLatitude>
                </Point>
                <Point>
                  <PointLongitude>180</PointLongitude>
                  <PointLatitude><xsl:value-of select="./NorthBoundingCoordinate" /></PointLatitude>
                </Point>
                <Point>
                  <PointLongitude>-135</PointLongitude>
                  <PointLatitude><xsl:value-of select="./NorthBoundingCoordinate" /></PointLatitude>
                </Point>
                <Point>
                  <PointLongitude>-90</PointLongitude>
                  <PointLatitude><xsl:value-of select="./NorthBoundingCoordinate" /></PointLatitude>
                </Point>
                <Point>
                  <PointLongitude>-45</PointLongitude>
                  <PointLatitude><xsl:value-of select="./NorthBoundingCoordinate" /></PointLatitude>
                </Point>
              </Boundary>
            </GPolygon>
          </xsl:when>
          <xsl:when test="((./NorthBoundingCoordinate &gt; 89.999) or (((./WestBoundingCoordinate &lt; -180.0) or (./EastBoundingCoordinate &gt; 180.0)) and (./SouthBoundingCoordinate &gt; 0.0)) or ((./WestBoundingCoordinate = -180.0) and (./EastBoundingCoordinate = 180.0) and (./SouthBoundingCoordinate &gt; 0.0)))">
            <!-- When a "too north" value is seen,
                 or a "too west" or "too east" value is seen in the
                 northern hemisphere,
                 form a clockwise ring around the north pole -->
            <GPolygon>
              <Boundary>
                <Point>
                  <PointLongitude>0</PointLongitude>
                  <PointLatitude><xsl:value-of select="./SouthBoundingCoordinate" /></PointLatitude>
                </Point>
                <Point>
                  <PointLongitude>-45</PointLongitude>
                  <PointLatitude><xsl:value-of select="./SouthBoundingCoordinate" /></PointLatitude>
                </Point>
                <Point>
                  <PointLongitude>-90</PointLongitude>
                  <PointLatitude><xsl:value-of select="./SouthBoundingCoordinate" /></PointLatitude>
                </Point>
                <Point>
                  <PointLongitude>-135</PointLongitude>
                  <PointLatitude><xsl:value-of select="./SouthBoundingCoordinate" /></PointLatitude>
                </Point>
                <Point>
                  <PointLongitude>-180</PointLongitude>
                  <PointLatitude><xsl:value-of select="./SouthBoundingCoordinate" /></PointLatitude>
                </Point>
                <Point>
                  <PointLongitude>135</PointLongitude>
                  <PointLatitude><xsl:value-of select="./SouthBoundingCoordinate" /></PointLatitude>
                </Point>
                <Point>
                  <PointLongitude>90</PointLongitude>
                  <PointLatitude><xsl:value-of select="./SouthBoundingCoordinate" /></PointLatitude>
                </Point>
                <Point>
                  <PointLongitude>45</PointLongitude>
                  <PointLatitude><xsl:value-of select="./SouthBoundingCoordinate" /></PointLatitude>
                </Point>
              </Boundary>
            </GPolygon>
          </xsl:when>
          <!-- Handle case where box is more than 180 degrees wide -->
          <xsl:when test="((./EastBoundingCoordinate - ./WestBoundingCoordinate) &gt; 180.0)">
            <BoundingRectangle>
              <xsl:call-template name="copy">
                <xsl:with-param name="nodeList" select="./WestBoundingCoordinate" />
              </xsl:call-template>
              <xsl:call-template name="copy">
                <xsl:with-param name="nodeList" select="./NorthBoundingCoordinate" />
              </xsl:call-template>
              <EastBoundingCoordinate>0.0</EastBoundingCoordinate>
              <xsl:call-template name="copy">
                <xsl:with-param name="nodeList" select="./SouthBoundingCoordinate" />
              </xsl:call-template>
            </BoundingRectangle>
            <BoundingRectangle>
              <WestBoundingCoordinate>0.0</WestBoundingCoordinate>
              <xsl:call-template name="copy">
                <xsl:with-param name="nodeList" select="./NorthBoundingCoordinate" />
              </xsl:call-template>
              <xsl:call-template name="copy">
                <xsl:with-param name="nodeList" select="./EastBoundingCoordinate" />
              </xsl:call-template>
              <xsl:call-template name="copy">
                <xsl:with-param name="nodeList" select="./SouthBoundingCoordinate" />
              </xsl:call-template>
            </BoundingRectangle>
          </xsl:when>
          <xsl:otherwise>
            <!--
            <xsl:call-template name="copy">
              <xsl:with-param name="nodeList" select="." />
            </xsl:call-template>
            -->
            <BoundingRectangle>
              <xsl:apply-templates select="./WestBoundingCoordinate" />
              <xsl:apply-templates select="./NorthBoundingCoordinate" />
              <xsl:apply-templates select="./EastBoundingCoordinate" />
              <xsl:apply-templates select="./SouthBoundingCoordinate" />
            </BoundingRectangle>
          </xsl:otherwise>
        </xsl:choose>
      </xsl:when> <!-- END when (ShortName eq 'AIRABRAD') or ... -->
      <!-- For other cases, expect the GranuleSpatialRepresentation
           to be 'Cartesian', where only a BoundingRectangle is used.
           If the BoundingRectangle crosses the 180 degree meridian,
           split it into two separate BoundingRectangles.
      -->
      <xsl:when test="(./HorizontalSpatialDomainContainer/BoundingRectangle/SouthBoundingCoordinate = ./HorizontalSpatialDomainContainer/BoundingRectangle/NorthBoundingCoordinate) and (./HorizontalSpatialDomainContainer/BoundingRectangle/WestBoundingCoordinate = ./HorizontalSpatialDomainContainer/BoundingRectangle/EastBoundingCoordinate)">
        <!-- Create a Point instead of a BoundingRectangle in this case -->
        <Point>
          <PointLongitude><xsl:value-of select="./WestBoundingCoordinate" /></PointLongitude>
          <PointLatitude><xsl:value-of select="./SouthBoundingCoordinate" /></PointLatitude>
        </Point>
      </xsl:when>
      <xsl:when test="(./WestBoundingCoordinate &gt; 0) and (./EastBoundingCoordinate &lt; 0)">
        <BoundingRectangle>
          <xsl:apply-templates select="./WestBoundingCoordinate" />
          <xsl:apply-templates select="./NorthBoundingCoordinate" />
          <EastBoundingCoordinate>180.0</EastBoundingCoordinate>
          <xsl:apply-templates select="./SouthBoundingCoordinate" />
        </BoundingRectangle>
        <BoundingRectangle>
          <WestBoundingCoordinate>-180.0</WestBoundingCoordinate>
          <xsl:apply-templates select="./NorthBoundingCoordinate" />
          <xsl:apply-templates select="./EastBoundingCoordinate" />
          <xsl:apply-templates select="./SouthBoundingCoordinate" />
        </BoundingRectangle>
      </xsl:when>
      <xsl:otherwise>
        <!--
        <xsl:call-template name="copy">
          <xsl:with-param name="nodeList" select="." />
        </xsl:call-template>
        -->
        <BoundingRectangle>
          <xsl:apply-templates select="./WestBoundingCoordinate" />
          <xsl:apply-templates select="./NorthBoundingCoordinate" />
          <xsl:apply-templates select="./EastBoundingCoordinate" />
          <xsl:apply-templates select="./SouthBoundingCoordinate" />
        </BoundingRectangle>
     </xsl:otherwise>
    </xsl:choose>
  </xsl:template>

  <xsl:template match="WestBoundingCoordinate">
    <xsl:choose>
      <xsl:when test="contains(.,'e-')">
        <!-- Replace value having negative exponent with zero -->
        <WestBoundingCoordinate>0.0</WestBoundingCoordinate>
      </xsl:when>
      <xsl:otherwise>
        <xsl:call-template name="copy">
          <xsl:with-param name="nodeList" select="." />
        </xsl:call-template>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template>

  <xsl:template match="NorthBoundingCoordinate">
    <xsl:choose>
      <xsl:when test="contains(.,'e-')">
        <!-- Replace value having negative exponent with zero -->
        <NorthBoundingCoordinate>0.0</NorthBoundingCoordinate>
      </xsl:when>
      <xsl:otherwise>
        <xsl:call-template name="copy">
          <xsl:with-param name="nodeList" select="." />
        </xsl:call-template>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template>

  <xsl:template match="EastBoundingCoordinate">
    <xsl:choose>
      <xsl:when test="contains(.,'e-')">
        <!-- Replace value having negative exponent with zero -->
        <EastBoundingCoordinate>0.0</EastBoundingCoordinate>
      </xsl:when>
      <xsl:otherwise>
        <xsl:call-template name="copy">
          <xsl:with-param name="nodeList" select="." />
        </xsl:call-template>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template>

  <xsl:template match="SouthBoundingCoordinate">
    <xsl:choose>
      <xsl:when test="contains(.,'e-')">
        <!-- Replace value having negative exponent with zero -->
        <SouthBoundingCoordinate>0.0</SouthBoundingCoordinate>
      </xsl:when>
      <xsl:otherwise>
        <xsl:call-template name="copy">
          <xsl:with-param name="nodeList" select="." />
        </xsl:call-template>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template>

  <xsl:template match="GPolygon">
    <Spatial>
      <xsl:apply-templates select="LocalityValue" />
      <xsl:apply-templates select="VerticalSpatialDomain" />
      <HorizontalSpatialDomain>
        <xsl:call-template name="copy">
          <xsl:with-param name="nodeList" select="./ZoneIdentifier" />
        </xsl:call-template>
        <Geometry>
          <xsl:call-template name="copy">
            <xsl:with-param name="nodeList" select="./HorizontalSpatialDomainContainer/GPolygon" />
          </xsl:call-template>
        </Geometry>
      </HorizontalSpatialDomain>
    </Spatial>
  </xsl:template>

  <xsl:template match="MeasuredParameter">
    <MeasuredParameter>
      <ParameterName><xsl:value-of select="./ParameterName" /></ParameterName>
      <QAStats>
        <xsl:apply-templates select="./QAPercentMissing" />
        <xsl:apply-templates select="./QAPercentOutofBounds" />
        <xsl:call-template name="copy">
          <xsl:with-param name="nodeList" select="./QAPercentInterpolatedData" />
        </xsl:call-template>
        <xsl:call-template name="copy">
          <xsl:with-param name="nodeList" select="./QAPercentCloudCover" />
        </xsl:call-template>
      </QAStats>
      <QAFlags>
        <xsl:call-template name="copy">
          <xsl:with-param name="nodeList" select="./AutomaticQualityFlag" />
        </xsl:call-template>
        <xsl:call-template name="copy">
          <xsl:with-param name="nodeList" select="./AutomaticQualityFlagExplanation" />
        </xsl:call-template>
        <xsl:call-template name="copy">
          <xsl:with-param name="nodeList" select="./OperationalQualityFlag" />
        </xsl:call-template>
        <xsl:call-template name="copy">
          <xsl:with-param name="nodeList" select="./OperationalQualityFlagExplanation" />
        </xsl:call-template>
        <xsl:call-template name="copy">
          <xsl:with-param name="nodeList" select="./ScienceQualityFlag" />
        </xsl:call-template>
        <xsl:call-template name="copy">
          <xsl:with-param name="nodeList" select="./ScienceQualityFlagExplanation" />
        </xsl:call-template>
      </QAFlags>
    </MeasuredParameter>
  </xsl:template>

  <xsl:template match="QAPercentMissing">
    <QAPercentMissingData>
      <xsl:value-of select="." />
    </QAPercentMissingData>
  </xsl:template>

  <xsl:template match="QAPercentOutofBounds">
    <QAPercentOutOfBoundsData>
      <xsl:value-of select="." />
    </QAPercentOutOfBoundsData>
  </xsl:template>

  <xsl:template match="Platform">
    <Platform>
      <ShortName>
        <xsl:value-of select="./PlatformShortName" />
      </ShortName>
      <Instruments>
        <xsl:apply-templates select="./Instrument" />
      </Instruments>
    </Platform>
  </xsl:template>
  <!--
  <xsl:template match="Instrument">
    <Instrument>
      <ShortName>
        <xsl:value-of select="./InstrumentShortName" />
      </ShortName>
      <Sensors>
        <xsl:apply-templates select="./Sensor" />
      </Sensors>
    </Instrument>
  </xsl:template>
  -->
  <!-- We do not expect the DIFs to have sensor information, so here we use
       the value of the instrument as the value of the sensor.
   -->
  <xsl:template match="Instrument">
    <Instrument>
      <ShortName>
        <xsl:value-of select="./InstrumentShortName" />
      </ShortName>
      <Sensors>
        <Sensor>
          <ShortName>
            <xsl:value-of select="./InstrumentShortName" />
          </ShortName>
        </Sensor>
      </Sensors>
    </Instrument>
  </xsl:template>

  <xsl:template match="Sensor">
    <Sensor>
      <ShortName>
        <xsl:value-of select="./SensorShortName" />
      </ShortName>
    </Sensor>
  </xsl:template>

  <xsl:template match="DataGranule/Format">
    <!-- Because of an oversight, AIRS data sets on airscal1, airscal2,
         and airspar1 have no value within the DataGranule/Format tag.
         Since the value is optional in ECHO, we will not create a DataFormat
         tag if DataGranule/Format is empty or consists only of whitespace.
    -->
    <xsl:if test="normalize-space(.)">
      <DataFormat><xsl:value-of select="." /></DataFormat>
    </xsl:if>
  </xsl:template>

  <xsl:template match="BrowseFile">
    <ProviderBrowseId><xsl:value-of select="." /></ProviderBrowseId>
  </xsl:template>

  <xsl:template match="OrbitNumber">
    <xsl:choose>
      <xsl:when test="substring(.,1,1) = '('">
        <!-- Remove any parentheses that surround OrbitNumber -->
        <OrbitNumber><xsl:value-of select="substring(., 2, string-length(.)-2)" /></OrbitNumber>
      </xsl:when>
      <xsl:otherwise>
        <OrbitNumber><xsl:value-of select="." /></OrbitNumber>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template>

  <xsl:template name="copy">
    <xsl:param name="nodeList" />
    <xsl:for-each select="$nodeList">
      <xsl:element name="{local-name()}">
        <xsl:for-each select="@*">
          <xsl:attribute name="{local-name()}">
            <xsl:value-of select="." />
          </xsl:attribute>
        </xsl:for-each>
        <xsl:value-of select="text()" />
        <xsl:call-template name="copy">
          <xsl:with-param name="nodeList" select="./child::*" />
        </xsl:call-template>
      </xsl:element>
    </xsl:for-each>
  </xsl:template>

  <!--
    This template ensures that the order of PointLongitude and PointLatitude
    is correct, even if the order is swapped in the file being transformed.
    -->
  <xsl:template name="copyGPolygons">
    <xsl:param name="nodeList" />
    <xsl:for-each select="$nodeList">
      <xsl:element name="GPolygon">
        <xsl:if test="Boundary">
          <xsl:element name="Boundary">
            <xsl:for-each select="Boundary/Point">
              <xsl:element name="Point">
                <xsl:copy-of select="PointLongitude" />
                <xsl:copy-of select="PointLatitude" />
              </xsl:element>
            </xsl:for-each>
          </xsl:element>
        </xsl:if>
        <xsl:if test="ExclusiveZone">
          <xsl:element name="ExclusiveZone">
            <xsl:for-each select="ExclusiveZone/Boundary">
              <xsl:element name="Boundary">
                <xsl:for-each select="Point">
                  <xsl:element name="Point">
                    <xsl:copy-of select="PointLongitude" />
                    <xsl:copy-of select="PointLatitude" />
                  </xsl:element>
                </xsl:for-each>
              </xsl:element>
            </xsl:for-each>
          </xsl:element>
        </xsl:if>
        <xsl:if test="CenterPoint">
          <xsl:element name="CenterPoint">
            <xsl:copy-of select="CenterPoint/PointLongitude" />
            <xsl:copy-of select="CenterPoint/PointLatitude" />
          </xsl:element>
        </xsl:if>
      </xsl:element>
    </xsl:for-each>
  </xsl:template>

</xsl:stylesheet>

