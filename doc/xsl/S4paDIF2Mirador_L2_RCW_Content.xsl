<?xml version="1.0" encoding="UTF-8"?>
<!-- $Id: S4paDIF2Mirador_L2_RCW_Content.xsl,v 1.4 2008/09/30 13:56:29 glei Exp $ -->
<!-- -@@@ S4PA, Version: $Name:  $ -->
<xsl:stylesheet
version="1.0" 
xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
>

<xsl:output
  method="xml"
  version="1.0"
  encoding="UTF-8"
  indent="yes"
  omit-xml-declaration="yes"
/>

<!--
This is an XSLT style sheet that converts metadata in DIF XML format to
Mirador Data Product Page HTML (XHTML) format.
It is intended to replace an editable instance in a dwt template that
has the name "Content".
-->

<xsl:template match="DIF">

<xsl:apply-templates select="Summary" />

</xsl:template>

<!-- =========================== End HTML =============================== -->
<!-- ==================================================================== -->

<!-- ================== Templates start here ==================== -->

<xsl:template match="Summary">
   <xsl:value-of select="." />
</xsl:template>

</xsl:stylesheet>
