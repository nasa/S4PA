<?xml version="1.0" encoding="utf-8"?>
<!-- $Id: S4paGran2ECHO.xsl,v 1.31 2010/05/13 04:45:47 eseiler Exp $ -->
<!-- -@@@ S4PA, Version $Name:  $ -->
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
  <xsl:output method="xml" indent="yes"/>
  <!-- This XSLT can be used to transform S4PA Granule level XML Meta-data -->
  <!-- files into ECHO ingest files. -->
  <xsl:template match="/">
    <xsl:for-each select="S4PAGranuleMetaDataFile">
      <GranuleMetaDataFile>
        <DTDVersion>1.0</DTDVersion>
        <DataCenterId>GSF</DataCenterId>
        <GranuleMetaDataSet>
          <Granules>
            <GranuleURMetaData>
              <GranuleUR><xsl:value-of select="CollectionMetaData/ShortName"/>.<xsl:value-of select="CollectionMetaData/VersionID"/>:<xsl:value-of select="DataGranule/GranuleID"/></GranuleUR>
              <InsertTime><xsl:value-of select="DataGranule/InsertDateTime"/></InsertTime>
              <LastUpdate><xsl:value-of select="DataGranule/InsertDateTime"/></LastUpdate>
              <CollectionMetaData>
                <xsl:call-template name="copy">
                  <xsl:with-param name="nodeList" select="CollectionMetaData/ShortName" />
                </xsl:call-template>
                <xsl:call-template name="copy">
                  <xsl:with-param name="nodeList" select="CollectionMetaData/VersionID" />
                </xsl:call-template>
              </CollectionMetaData>
              <DataGranule>
                <SizeMBDataGranule><xsl:value-of select="DataGranule/SizeBytesDataGranule div 1024.0" /></SizeMBDataGranule>
                <ProducerGranuleID><xsl:value-of select="DataGranule/GranuleID"/></ProducerGranuleID>
                <xsl:choose>
                  <xsl:when test="DataGranule/DayNightFlag">
                    <xsl:call-template name="copy">
                      <xsl:with-param name="nodeList" select="DataGranule/DayNightFlag" />
                    </xsl:call-template>
                  </xsl:when>
                  <xsl:otherwise>
                    <DayNightFlag>UNSPECIFIED</DayNightFlag>
                  </xsl:otherwise>
                </xsl:choose>
                <xsl:choose>
                  <xsl:when test="DataGranule/ProductionDateTime">
                    <xsl:choose>
                      <xsl:when test="substring(DataGranule/ProductionDateTime,11,1) = 'T'">
                        <!-- If the format is like 2007-04-12T23:37:57.000Z,
                             convert to "2007-04-12 23:37:57" -->
                        <ProductionDateTime><xsl:value-of select="substring-before(DataGranule/ProductionDateTime, 'T')" /><xsl:text> </xsl:text><xsl:value-of select="substring(substring-after(DataGranule/ProductionDateTime, 'T'), 1, 8)" /></ProductionDateTime>
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
                      <xsl:when test="substring(DataGranule/InsertDateTime,11,1) = 'T'">
                        <!-- If the format is like 2007-04-12T23:37:57.000Z,
                             convert to "2007-04-12 23:37:57" -->
                        <ProductionDateTime><xsl:value-of select="substring-before(DataGranule/InsertDateTime, 'T')" /><xsl:text> </xsl:text><xsl:value-of select="substring(substring-after(DataGranule/InsertDateTime, 'T'), 1, 8)" /></ProductionDateTime>
                      </xsl:when>
                      <xsl:otherwise>
                        <ProductionDateTime><xsl:value-of select="DataGranule/InsertDateTime"/></ProductionDateTime>
                      </xsl:otherwise>
                    </xsl:choose>
                  </xsl:otherwise>
                </xsl:choose>
              </DataGranule>
              <RangeDateTime>
                <xsl:call-template name="copy">
                  <xsl:with-param name="nodeList" select="RangeDateTime/RangeEndingTime" />
                </xsl:call-template>
                <xsl:call-template name="copy">
                  <xsl:with-param name="nodeList" select="RangeDateTime/RangeEndingDate" />
                </xsl:call-template>
                <xsl:call-template name="copy">
                  <xsl:with-param name="nodeList" select="RangeDateTime/RangeBeginningTime" />
                </xsl:call-template>
                <xsl:call-template name="copy">
                  <xsl:with-param name="nodeList" select="RangeDateTime/RangeBeginningDate" />
                </xsl:call-template>
              </RangeDateTime>
              <xsl:choose>
                <!-- The only known datasets which require an
                     OrbitCalculatedSpatialDomain are OMI L2 orbital swath
                     datsets, which can be identified by the ShortName
                     in the CollectionMetaData.
                -->
                <xsl:when test="(CollectionMetaData/ShortName/text() = 'OMAERUV') or (CollectionMetaData/ShortName/text() = 'OMBRO') or (CollectionMetaData/ShortName/text() = 'OMCLDO2') or (CollectionMetaData/ShortName/text() = 'OMCLDRR') or (CollectionMetaData/ShortName/text() = 'OMDOAO3') or (CollectionMetaData/ShortName/text() = 'OMHCHO') or (CollectionMetaData/ShortName/text() = 'OMNO2') or (CollectionMetaData/ShortName/text() = 'OMOCLO') or (CollectionMetaData/ShortName/text() = 'OMSO2') or (CollectionMetaData/ShortName/text() = 'OMTO3') or (CollectionMetaData/ShortName/text() = 'OMAERO') or (CollectionMetaData/ShortName/text() = 'OMAEROZ') or (CollectionMetaData/ShortName/text() = 'OMPROO3') or (CollectionMetaData/ShortName/text() = 'OMUVB') or (CollectionMetaData/ShortName/text() = 'OML1BCAL') or (CollectionMetaData/ShortName/text() = 'OML1BIRR') or (CollectionMetaData/ShortName/text() = 'OML1BRUG') or (CollectionMetaData/ShortName/text() = 'OML1BRUZ') or (CollectionMetaData/ShortName/text() = 'OML1BRVG') or (CollectionMetaData/ShortName/text() = 'OML1BRVZ') or (CollectionMetaData/ShortName/text() = 'OMO3PR')">
                  <SpatialDomainContainer>
                    <HorizontalSpatialDomainContainer>
                      <ZoneIdentifier>Text</ZoneIdentifier>
                      <xsl:call-template name="copy">
                        <xsl:with-param name="nodeList" select="OrbitCalculatedSpatialDomain/OrbitCalculatedSpatialDomainContainer/Orbit" />
                      </xsl:call-template>
                    </HorizontalSpatialDomainContainer>
                  </SpatialDomainContainer>
                </xsl:when>
                <!-- Special case: MLS L1 and SORCE granules do not have a
                     SpatialDomainContainer, so create one -->
                <xsl:when test="(CollectionMetaData/ShortName/text() = 'ML1OA') or (CollectionMetaData/ShortName/text() = 'ML1RADD')">
                  <SpatialDomainContainer>
                    <HorizontalSpatialDomainContainer>
                      <BoundingRectangle>
                        <WestBoundingCoordinate>-180</WestBoundingCoordinate>
                        <NorthBoundingCoordinate>82</NorthBoundingCoordinate>
                        <EastBoundingCoordinate>180</EastBoundingCoordinate>
                        <SouthBoundingCoordinate>-82</SouthBoundingCoordinate>
                      </BoundingRectangle>
                    </HorizontalSpatialDomainContainer>
                  </SpatialDomainContainer>
                </xsl:when>
                <xsl:when test="(CollectionMetaData/ShortName/text() = 'ML1RADG') or (CollectionMetaData/ShortName/text() = 'ML1RADT') or (CollectionMetaData/ShortName/text() = 'SOR3TSI6') or (CollectionMetaData/ShortName/text() = 'SOR3TSID') or (CollectionMetaData/ShortName/text() = 'SOR3SSID') or (CollectionMetaData/ShortName/text() = 'UARSO3BS') or (CollectionMetaData/ShortName/text() = 'UARSU3BS')">
                  <SpatialDomainContainer>
                    <HorizontalSpatialDomainContainer>
                      <BoundingRectangle>
                        <WestBoundingCoordinate>-180</WestBoundingCoordinate>
                        <NorthBoundingCoordinate>90</NorthBoundingCoordinate>
                        <EastBoundingCoordinate>180</EastBoundingCoordinate>
                        <SouthBoundingCoordinate>-90</SouthBoundingCoordinate>
                      </BoundingRectangle>
                    </HorizontalSpatialDomainContainer>
                  </SpatialDomainContainer>
                </xsl:when>
                <xsl:otherwise>
                  <!-- All other datasets that have a SpatialDomainContainer
                       in the xml being transformed will include a
                       SpatialDomainContainer in the transformed output.
                   -->
                  <xsl:apply-templates select="SpatialDomainContainer" />
                </xsl:otherwise>
              </xsl:choose>
              <xsl:call-template name="copy">
                <xsl:with-param name="nodeList" select="OrbitCalculatedSpatialDomainContainer" />
              </xsl:call-template>
              <OnlineAccessURLs/>
              <xsl:apply-templates select="DataGranule/Format" />
            </GranuleURMetaData>
          </Granules>
        </GranuleMetaDataSet>
      </GranuleMetaDataFile>
    </xsl:for-each>
  </xsl:template>

  <xsl:template match="SpatialDomainContainer">
    <xsl:choose>
      <!--
           Only create a SpatialDomainContainer if we have a
           HorizontalSpatialDomainContainer containing a
           GPolygon or a BoundingRectangle
      -->
      <xsl:when test="(./HorizontalSpatialDomainContainer/GPolygon) or (./HorizontalSpatialDomainContainer/BoundingRectangle)">
        <xsl:choose>
          <xsl:when test="(./HorizontalSpatialDomainContainer/BoundingRectangle/SouthBoundingCoordinate = 0) and (./HorizontalSpatialDomainContainer/BoundingRectangle/NorthBoundingCoordinate = 0) and (./HorizontalSpatialDomainContainer/BoundingRectangle/WestBoundingCoordinate = 0) and (./HorizontalSpatialDomainContainer/BoundingRectangle/EastBoundingCoordinate = 0)">
            <!-- Do not create a SpatialDomainContainer in this case -->
          </xsl:when>
          <xsl:otherwise>
            <SpatialDomainContainer>
              <xsl:apply-templates select="LocalityValue" />
              <xsl:call-template name="copy">
                <xsl:with-param name="nodeList" select="./VerticalSpatialDomain" />
              </xsl:call-template>
              <HorizontalSpatialDomainContainer>
                <xsl:call-template name="copy">
                  <xsl:with-param name="nodeList" select="./ZoneIdentifier" />
                </xsl:call-template>
                <xsl:choose>
                  <xsl:when test="./HorizontalSpatialDomainContainer/GPolygon">
                    <xsl:call-template name="copy">
                      <xsl:with-param name="nodeList" select="./HorizontalSpatialDomainContainer/GPolygon" />
                    </xsl:call-template>
                  </xsl:when>
                  <xsl:otherwise>
                    <xsl:apply-templates select="./HorizontalSpatialDomainContainer/BoundingRectangle" />
                  </xsl:otherwise>
                </xsl:choose>
              </HorizontalSpatialDomainContainer>
            </SpatialDomainContainer>
          </xsl:otherwise>
        </xsl:choose>
      </xsl:when>
    </xsl:choose>
  </xsl:template>

  <xsl:template match="LocalityValue">
    <GranuleLocality>
      <xsl:copy-of select="." />
    </GranuleLocality>
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
            <xsl:call-template name="copy">
              <xsl:with-param name="nodeList" select="." />
            </xsl:call-template>
          </xsl:otherwise>
        </xsl:choose>
      </xsl:when> <!-- END when (ShortName eq 'AIRABRAD') or ... -->
      <!-- For other cases, expect the GranuleSpatialRepresentation
           to be 'Cartesian', where only a BoundingRectangle is used.
           If the BoundingRectangle crosses the 180 degree meridian,
           split it into two separate BoundingRectangles.
      -->
      <xsl:when test="(./WestBoundingCoordinate &gt; 0) and (./EastBoundingCoordinate &lt; 0)">
        <BoundingRectangle>
          <xsl:call-template name="copy">
            <xsl:with-param name="nodeList" select="./WestBoundingCoordinate" />
          </xsl:call-template>
          <xsl:call-template name="copy">
            <xsl:with-param name="nodeList" select="./NorthBoundingCoordinate" />
          </xsl:call-template>
          <EastBoundingCoordinate>180.0</EastBoundingCoordinate>
          <xsl:call-template name="copy">
            <xsl:with-param name="nodeList" select="./SouthBoundingCoordinate" />
          </xsl:call-template>
        </BoundingRectangle>
        <BoundingRectangle>
          <WestBoundingCoordinate>-180.0</WestBoundingCoordinate>
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
          <xsl:choose>
            <xsl:when test="contains(./WestBoundingCoordinate,'e-')">
              <!-- Replace value having negative exponent with zero -->
              <WestBoundingCoordinate>0.0</WestBoundingCoordinate>
            </xsl:when>
            <xsl:otherwise>
              <xsl:call-template name="copy">
                <xsl:with-param name="nodeList" select="./WestBoundingCoordinate" />
              </xsl:call-template>
            </xsl:otherwise>
          </xsl:choose>
          <xsl:choose>
            <xsl:when test="contains(./NorthBoundingCoordinate,'e-')">
              <!-- Replace value having negative exponent with zero -->
              <NorthBoundingCoordinate>0.0</NorthBoundingCoordinate>
            </xsl:when>
            <xsl:otherwise>
              <xsl:call-template name="copy">
                <xsl:with-param name="nodeList" select="./NorthBoundingCoordinate" />
              </xsl:call-template>
            </xsl:otherwise>
          </xsl:choose>
          <xsl:choose>
            <xsl:when test="contains(./EastBoundingCoordinate,'e-')">
              <!-- Replace value having negative exponent with zero -->
              <EastBoundingCoordinate>0.0</EastBoundingCoordinate>
            </xsl:when>
            <xsl:otherwise>
              <xsl:call-template name="copy">
                <xsl:with-param name="nodeList" select="./EastBoundingCoordinate" />
              </xsl:call-template>
            </xsl:otherwise>
          </xsl:choose>
          <xsl:choose>
            <xsl:when test="contains(./SouthBoundingCoordinate,'e-')">
              <!-- Replace value having negative exponent with zero -->
              <SouthBoundingCoordinate>0.0</SouthBoundingCoordinate>
            </xsl:when>
            <xsl:otherwise>
              <xsl:call-template name="copy">
                <xsl:with-param name="nodeList" select="./SouthBoundingCoordinate" />
              </xsl:call-template>
            </xsl:otherwise>
          </xsl:choose>
        </BoundingRectangle>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template>

  <xsl:template match="GPolygon">
    <SpatialDomainContainer>
      <xsl:apply-templates select="LocalityValue" />
      <xsl:call-template name="copy">
        <xsl:with-param name="nodeList" select="./VerticalSpatialDomain" />
      </xsl:call-template>
      <HorizontalSpatialDomainContainer>
        <xsl:call-template name="copy">
          <xsl:with-param name="nodeList" select="./ZoneIdentifier" />
        </xsl:call-template>
        <xsl:call-template name="copy">
          <xsl:with-param name="nodeList" select="./HorizontalSpatialDomainContainer/GPolygon" />
        </xsl:call-template>
      </HorizontalSpatialDomainContainer>
    </SpatialDomainContainer>
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

</xsl:stylesheet>
