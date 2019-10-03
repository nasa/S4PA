#!/usr/bin/perl

=head1 NAME

s4pa_check_instance_integrity.pl - the station script for Check<data class> stations in
S4PA.

=head1 SYNOPSIS

s4pa_check_instance_integrity.pl - checks the integrity of entire data in a 
given instance.

=head1 ABSTRACT

B<Pseudo code:>

    End

=head1 DESCRIPTION

s4pa_check_instance_integrity.pl checks the integrity of entire data in a given
instance of S4PA. It creates a thread per data class and monitors the thread
for progress. It is a Perl/Tk based GUI.

=head1 SEE ALSO

L<S4PA::Storage>

=cut

################################################################################
# $Id: s4pa_check_instance_integrity.pl,v 1.19 2008/06/02 17:27:50 ffang Exp $
# -@@@ S4PA, Version $Name:  $
################################################################################
use threads;
use threads::shared;
use Thread::Queue;
use Tk;
use Tk::ROText;
use strict;
use S4P;
use S4PA;
use S4PA::Storage;
use Safe;
use Getopt::Std;
use vars qw( $opt_r $opt_d );

getopts( 'r:d:' );

# Shared variable to indicate whether the "Start" button has been pressed
my $statusFlag : shared;

# Root directory of S4PA stations
my $s4paRoot = $opt_r;
my $logDir = $opt_d;
my $id = 'P' . $$ . 'T' . time();

# Find all data classes in the S4PA instance
my $stationCfg = $s4paRoot . "/storage/dataset.cfg";
my $cfg = new Safe "S4PA";
$cfg->rdo( $stationCfg )  or
S4P::perish( 1, "Cannot read station config file $stationCfg ($!)" );    
my %dummyHash;
@dummyHash{values(%S4PA::data_class)} = 1;
my @dataClassList = sort keys %dummyHash;

# Status flag set to 0 initially to indicate that no buttons have been 
# activated.
$statusFlag = 0;

# Lists of threads and queues
my ( $threadList, $queueList );
# Create a thread and a queue per data class
foreach my $dataClass ( @dataClassList ) {
    $queueList->{$dataClass} = Thread::Queue->new();
    $threadList->{$dataClass} = threads->create( "IntegrityChecker",
        DATA_CLASS => $dataClass, S4PA_ROOT => $s4paRoot,
        QUEUE => $queueList->{$dataClass}, LOGDIR => $logDir, ID => $id );
}

# Create the GUI
my $mainWin = MainWindow->new( -title => 'Check Instance Integrity' );

# A hash to hold LED
my ( $led ) = {};
{
    use vars qw( $darkRedLED $lightRedLED $darkGreenLED $lightGreenLED );
    local( $/ ) = undef;
    my $str = <DATA>;
    my $cfg = new Safe 'ICON';
    $cfg->reval( $str );
    $led->{dark_red} = $mainWin->Pixmap( -data => $ICON::darkRedLED );
    $led->{light_red} = $mainWin->Pixmap( -data => $ICON::lightRedLED );
    $led->{dark_green} = $mainWin->Pixmap( -data => $ICON::darkGreenLED );
    $led->{light_green} = $mainWin->Pixmap( -data => $ICON::lightGreenLED );
}

# Title label
my $titleFrame = $mainWin->Frame()->pack( -anchor => 'w' );

# Data Class display in left column
my $descFrame = $mainWin->Frame( -borderwidth => 2, 
    -relief => 'flat' )->pack( -anchor => 'w' );
my $col = 0;
foreach my $title ( 'Data Class', 'Dataset Name', 'Version', 'Files Scanned',
    'Progress', 'Status', 'Report' ) {
    my $label = $descFrame->Label( -text => $title,
        -background => 'grey' );
    $label->grid( -row => 0, -column => $col++, -sticky => 'nsew' );
    $descFrame->Canvas( -height => 1, -width => 1 )->grid( -row => 0, 
        -column => $col++ );
}
my $colSpan = $col;
my $row = 1;
# A hash ref to hold label widgets; first level key is the data class name,
# second level keys are dataset, version and progress.
my $widgetVar = {};
my $statusHash = {};
my $toggle = 0;
foreach my $dataClass ( @dataClassList ) {
    # Stop all jobs and stations related to CheckIntegrity
    my $stationDir = "$s4paRoot/storage/$dataClass/check_$dataClass";
    my $oldDir = chdir( $stationDir );
    local( *FH );
    if ( opendir( FH, "." ) ) {
        my @jobList = grep( /RUNNING/, readdir( FH ) );
        foreach my $job ( @jobList ) {
            S4P::terminate_job( $job );
        }
        closedir( FH );
    }
    S4P::stop_station( $stationDir );
    chdir( $oldDir );
    $descFrame->Label( -text => $dataClass )->grid( -row => $row, -column => 0, -sticky => 'nsew' );
    $descFrame->Label( -textvariable => \$widgetVar->{$dataClass}{dataset} 
        )->grid( -row => $row, -column => 2, -sticky => 'nsew' );
    $descFrame->Label( -textvariable => \$widgetVar->{$dataClass}{version}
        )->grid( -row => $row, -column => 4, -sticky => 'nsew' );
    $descFrame->Label( -textvariable => \$widgetVar->{$dataClass}{fileCount}
        )->grid( -row => $row, -column => 6, -sticky => 'nsew' );
    $descFrame->Label( -textvariable => \$widgetVar->{$dataClass}{progress}
        )->grid( -row => $row, -column => 8, -sticky => 'nsew' );
    $widgetVar->{$dataClass}{status} = $descFrame->Label()->grid( -row => $row, -column => 10, -sticky => 'nsew' );
    $descFrame->Button( -text => 'View',
        -command => sub {
            S4PA::ViewFile( PARENT => $mainWin,
                TITLE => "CheckIntegrity: $dataClass",
                FILE => "$logDir/$dataClass.$id.log" );
            } )->grid( -row => $row, -column => 12, -sticky => 'nsew' );
    $statusHash->{$dataClass} = 0;
    $row++;
}
# A dummy separator
$mainWin->Frame( -height => 2, -relief => 'ridge',
    -background => 'black' )->pack( -fill => 'x' );
    
# Ok/Cancel buttons                           
my $botFrame = $mainWin->Frame()->pack( -fill => 'x' );

my $startButton = $botFrame->Button( -text => 'Start' );
$startButton->configure( -command => [
    sub{ $startButton->configure( -state => 'disabled' ); $statusFlag = 1;} ] );
$startButton->grid( -column => 0, -row => 0 );
my $stopButton = $botFrame->Button( -text => 'Stop', 
    -command => [ sub {
        return unless ( $statusFlag == 1 );
        $statusFlag = -1;
        foreach my $thread ( threads->list() ) {
            $thread->join();
        }
    } ] );
$stopButton->grid( -column => 1, -row => 0 );
my $exitButton = $botFrame->Button( -text => 'Exit', 
    -command => [ sub {
        $statusFlag = -1;
        foreach my $thread ( threads->list() ) {
            $thread->join();
        }
        exit(0);
    } ] );
$exitButton->grid( -column => 2, -row => 0 );
$mainWin->repeat( 200 => sub {
    UpdateMainWindow( QUEUE_LIST => $queueList, WIDGET_VARS =>$widgetVar,
        WINDOW => $mainWin, LED => $led );
    } );
MainLoop;
################################################################################
sub UpdateMainWindow
{
    my ( %arg ) = @_;
    my $queueList = $arg{QUEUE_LIST};
    my $widgetVar = $arg{WIDGET_VARS};
    my $window = $arg{WINDOW};
    return unless $statusFlag;
    foreach my $dataClass ( sort keys %$widgetVar ) {
        my $count = $queueList->{$dataClass}->pending();
        LOOP: while( $count-- ) {
            my $val = $queueList->{$dataClass}->dequeue();
            last LOOP unless defined $val;
            if ( $val =~ /^INIT/ ) {
                $val =~ s/^INIT:\s*//;
                my ( $dataset, $version ) = split( /,/, $val );
                $widgetVar->{$dataClass}{dataset} = $dataset;
                $widgetVar->{$dataClass}{version} = $version;
                $statusHash->{$dataClass} = 1;
            } elsif ( $val =~ /^ERROR/ ) {
               $statusHash->{$dataClass} = 2;
            } elsif ( $val =~ /^UPDATE/ ) {
                $val =~ s/^UPDATE:\s*//;
                my ( $count, $progress ) = split( /,/, $val );
                $widgetVar->{$dataClass}{fileCount} = $count;
                $widgetVar->{$dataClass}{progress} = "$progress\%";
            } elsif ( $val =~ /DONE:\s*(\d)/ ) {
                my $exitStatus = $1;
                $statusHash->{$dataClass} = ( $exitStatus ? 3 : 4 );
            }
            my $status = $statusHash->{$dataClass};            
            my $color = ( $status == 4 ) ? 'light_red'
                : ( $status == 3 ) ? 'light_green'
                : ( $status == 2 ) ? 'red'
                : ( $status == 1 ) ? 'green'
                : undef;
            
            if ( defined $color ) {                
                my $key = $color;
                if ( $status < 3 ) {
                    $key = ($toggle%2) ? "dark_$color" : "light_$color";
                }
                my $led = $arg{LED}->{$key};
                $widgetVar->{$dataClass}{status}->configure( -image => $led, -height => 8, -width => 8 );
            }
        }
        $window->update();
    }
    $toggle++;
}
################################################################################
sub IntegrityChecker
{
    my ( %arg ) = @_;

    my $dataClass = $arg{DATA_CLASS};
    my $checkIntegrityStationConfig = "$arg{S4PA_ROOT}/storage/"
        . "$arg{DATA_CLASS}/check_$arg{DATA_CLASS}/s4pa_check_integrity.cfg";
    my $queue = $arg{QUEUE};
    my $id = $arg{ID};
    my %cfg_data_version = ReadConfigFile( $checkIntegrityStationConfig );
    my @datasetList = keys %cfg_data_version;
    while( 1 ) {
        # Case of 'Start' button pressed
        last if $statusFlag;
        # Case of 'Exit' button pressed
        return if ( $statusFlag == -1 );
    }
    S4P::redirect_log( "$arg{LOGDIR}/$dataClass.$id.log" );
    foreach my $dataset ( @datasetList ) {
        my @versionList = @{$cfg_data_version{$dataset}};
        my $storageDir = "$arg{S4PA_ROOT}/storage/$arg{DATA_CLASS}";
        foreach my $version ( @versionList ) {
            # Case of 'Exit' button pressed
            return if ( $statusFlag == -1 );
            $queue->enqueue( "INIT:$dataset,$version" );
            my $dataVersion = $dataset . ( $version eq '' ? '' : ".$version" );
            my $status = S4PA::Storage::CheckCRC( DATASET => $dataVersion,
                CONTINUE_ON_ERROR => 1, QUEUE => $queue,
                STORAGE => $storageDir, INTERRUPT_FLAG => \$statusFlag,
                VERIFY_CKSUM => 1 );
            $queue->enqueue( "DONE:$status" );
        }
    }
    $queue->enqueue( undef );
}
################################################################################
sub ReadConfigFile
{
    my ( $file ) = @_;
    return undef unless ( -f $file );
    my $cpt = new Safe 'CFG';
    unless( $cpt->rdo( $file ) ) {
    }
    return %CFG::cfg_data_version;
}
################################################################################
__DATA__

$darkGreenLED = <<'EOF';
/* XPM */
static char * dark_green_xpm[] = {
"32 32 215 2",
"  	c #FFFFFF",
". 	c #FEFFFE",
"+ 	c #F9FCF9",
"@ 	c #F7FBF7",
"# 	c #F6FBF6",
"$ 	c #F5FAF5",
"% 	c #F5FAF4",
"& 	c #FDFEFD",
"* 	c #F8FCF8",
"= 	c #F3F9F2",
"- 	c #DDEEDB",
"; 	c #D6EBD4",
"> 	c #D5EAD3",
", 	c #D3E9D1",
"' 	c #CFE7CD",
") 	c #CCE5C9",
"! 	c #C7E3C4",
"~ 	c #C2E1BF",
"{ 	c #CAE4C7",
"] 	c #EAF5E9",
"^ 	c #F4F9F3",
"/ 	c #FCFEFC",
"( 	c #FAFDFA",
"_ 	c #DEEEDC",
": 	c #BCDEB9",
"< 	c #B9DCB6",
"[ 	c #B5DAB1",
"} 	c #AED7AA",
"| 	c #A6D3A2",
"1 	c #9ECF99",
"2 	c #95CA90",
"3 	c #98CC93",
"4 	c #B3D9AF",
"5 	c #C0E0BD",
"6 	c #E8F4E7",
"7 	c #E6F3E5",
"8 	c #D7EBD5",
"9 	c #C4E2C1",
"0 	c #C3E1C0",
"a 	c #BEDFBB",
"b 	c #B8DCB5",
"c 	c #B1D8AD",
"d 	c #A7D3A3",
"e 	c #93C98E",
"f 	c #8BC585",
"g 	c #86C380",
"h 	c #88C482",
"i 	c #ADD6A9",
"j 	c #EEF6ED",
"k 	c #DAECD8",
"l 	c #C8E3C5",
"m 	c #C1E0BE",
"n 	c #C9E4C6",
"o 	c #CEE7CC",
"p 	c #D0E8CE",
"q 	c #B0D8AC",
"r 	c #A4D2A0",
"s 	c #9ACD95",
"t 	c #8FC789",
"u 	c #82C17C",
"v 	c #78BC71",
"w 	c #79BC72",
"x 	c #90C78A",
"y 	c #ECF6EB",
"z 	c #DCEDDA",
"A 	c #BDDEBA",
"B 	c #CBE5C8",
"C 	c #B6DAB2",
"D 	c #AAD5A6",
"E 	c #9FCF9A",
"F 	c #92C98D",
"G 	c #7ABD73",
"H 	c #6EB767",
"I 	c #68B461",
"J 	c #7BBE75",
"K 	c #EDF6EC",
"L 	c #BADDB7",
"M 	c #C6E3C3",
"N 	c #D1E8CF",
"O 	c #E5F2E4",
"P 	c #E9F4E8",
"Q 	c #A2D19D",
"R 	c #89C483",
"S 	c #6FB768",
"T 	c #63B15B",
"U 	c #5FAF57",
"V 	c #83C17D",
"W 	c #DBEDD9",
"X 	c #FBFDFB",
"Y 	c #CDE6CB",
"Z 	c #AFD7AB",
"` 	c #E1F0E0",
" .	c #A4D19F",
"..	c #97CB92",
"+.	c #7DBE77",
"@.	c #71B86A",
"#.	c #64B25C",
"$.	c #58AC50",
"%.	c #5DAE55",
"&.	c #A0D09B",
"*.	c #A5D2A1",
"=.	c #57AB4F",
"-.	c #4EA745",
";.	c #67B35F",
">.	c #D4EAD2",
",.	c #56AB4E",
"'.	c #4AA541",
").	c #6DB666",
"!.	c #61B059",
"~.	c #54AA4C",
"{.	c #48A43F",
"].	c #3F9F35",
"^.	c #9BCD96",
"/.	c #76BB6F",
"(.	c #69B562",
"_.	c #52A949",
":.	c #45A23C",
"<.	c #399C2F",
"[.	c #50A847",
"}.	c #A8D4A4",
"|.	c #7CBE76",
"1.	c #66B35E",
"2.	c #59AC51",
"3.	c #41A138",
"4.	c #369B2C",
"5.	c #E7F3E6",
"6.	c #8CC686",
"7.	c #96CB91",
"8.	c #B7DBB3",
"9.	c #81C07B",
"0.	c #77BB70",
"a.	c #6BB564",
"b.	c #60B058",
"c.	c #49A440",
"d.	c #3E9F34",
"e.	c #329928",
"f.	c #4CA643",
"g.	c #8AC584",
"h.	c #7BBD74",
"i.	c #8DC687",
"j.	c #9DCE98",
"k.	c #A9D4A5",
"l.	c #84C27E",
"m.	c #65B25D",
"n.	c #5AAD52",
"o.	c #4FA746",
"p.	c #44A23B",
"q.	c #2D9723",
"r.	c #D9ECD7",
"s.	c #72B96B",
"t.	c #91C88C",
"u.	c #67B460",
"v.	c #5EAF56",
"w.	c #53AA4B",
"x.	c #339929",
"y.	c #28941D",
"z.	c #43A23A",
"A.	c #D8ECD6",
"B.	c #E4F2E3",
"C.	c #7EBF78",
"D.	c #7FBF79",
"E.	c #90C88B",
"F.	c #55AB4D",
"G.	c #389C2E",
"H.	c #2C9622",
"I.	c #229117",
"J.	c #74BA6D",
"K.	c #3A9D30",
"L.	c #309826",
"M.	c #26931B",
"N.	c #1F8F14",
"O.	c #F1F8F0",
"P.	c #ABD5A7",
"Q.	c #75BA6E",
"R.	c #5CAE54",
"S.	c #4DA644",
"T.	c #3C9E32",
"U.	c #1E8F13",
"V.	c #2A951F",
"W.	c #62B15A",
"X.	c #6AB563",
"Y.	c #4BA542",
"Z.	c #3B9D31",
"`.	c #209015",
" +	c #1A8D0F",
".+	c #CCE6CA",
"++	c #EFF7EE",
"@+	c #46A33D",
"#+	c #40A037",
"$+	c #319927",
"%+	c #29941E",
"&+	c #219016",
"*+	c #198D0E",
"=+	c #2A9520",
"-+	c #47A33E",
";+	c #51A848",
">+	c #359A2B",
",+	c #2E9724",
"'+	c #188C0D",
")+	c #5BAD53",
"!+	c #3FA036",
"~+	c #249219",
"{+	c #1D8F12",
"]+	c #F0F7EF",
"^+	c #379B2D",
"/+	c #349A2A",
"(+	c #25921A",
"_+	c #178C0C",
":+	c #2B9621",
"<+	c #239118",
"[+	c #DFEFDD",
"}+	c #A3D19E",
"|+	c #2F9825",
"1+	c #27931C",
"2+	c #EBF5EA",
"                                                                ",
"                    . + @ @ @ @ # # $ % # & .                   ",
"  . . . . . . . & * = - ; ; > , ' ) ! ~ { ] ^ / . . . . . . .   ",
"  . . . . . . ( ^ _ ; ~ : : < [ } | 1 2 3 4 5 6 @ & . . . . .   ",
"  . . . . & $ 7 8 9 0 0 0 ~ a b c d 1 e f g h i ' j / . . . .   ",
"  . . . & % k l m 0 n o p o { ~ < q r s t u v w x < y / . . .   ",
"  . . . * z A < m B , k - k , B m C D E F g G H I J : = . . .   ",
"  . . & K 5 4 L M N z O P O z N M L } Q 2 R J S T U V W X . .   ",
"  . . $ Y Z c A n > ` K = K ` > n : q  ...f +.@.#.$.%.&.K . .   ",
"  . / 6 Z *.q : n > ` K = K ` > n : q  ...f +.@.#.=.-.;.>.( .   ",
"  . % M &.Q } L M N z O P O z N M L } Q 2 R J S T ,.'.-.s K .   ",
"  . y r 2 E D C m B , k - k , B m C D E F g G ).!.~.{.].U _ &   ",
"  . ] ^.t s r q < ~ { o p o { ~ < q r s t u /.(.%._.:.<.[.k &   ",
"  . P 2 R e 1 }.c b a ~ 9 ~ a b c }.1 e R |.@.1.2.-.3.4.-.k &   ",
"  . 5.x u 6.7.E d i 4 C 8.C 4 i d E 7.6.9.0.a.b.~.c.d.e.f.k &   ",
"  . 7 g.h.V i.2 j.Q | k.D k.| Q j.2 i.l.G S m.n.o.p.<.q.{.r.&   ",
"  & O l.s.h.V f t.7.^.j.1 j.^.7.t.f V h.@.u.v.w.c.d.x.y.z.A.&   ",
"  & B.C.(.@.w D.g f t E.t.E.t f g D.w @.I b.F.f.3.G.H.I.].8 &   ",
"  . O 9.T ;.H J.G C.9.V l.V 9.C.G J.H ;.U ,.-.p.K.L.M.N.'.k &   ",
"  . O.P.;.%.T I H s.Q.0.v 0.Q.s.H I T R.F.S.p.T.e.y.U.V.g.] .   ",
"  . X k J.F.$.%.W.1.I X.X.X.I 1.W.%.=._.Y.z.Z.e.V.`. +z..+( .   ",
"  . . ++r %.-._.,.2.R.v.v.v.R.2.,._.f.@+#+<.$+%+&+*+=+h P . .   ",
"  . . X z +.S.-+'.-.[.;+_.;+[.-.'.@+#+T.>+,+y.N.'+U.)+N ( . .   ",
"  . . . = 8.u.-+!+#+z.p.:.p.z.#+d.K.>+L.V.~+{+'+N.@+| ]+. . .   ",
"  . . . / P i s.c.G.^+G.G.G.4./+$+q.V.(+N.*+_+V.2.1 B./ . . .   ",
"  . . . . / ] ~ t -.T.L.:+:+V.y.(+I.U. +*+<+>++.< 5.X . . . .   ",
"  . . . . . / $ [+}+l.Y.4.x.e.|+q.=+y.1+K.v ..W ^ / . . . . .   ",
"  . . . . . . . X ++-  .t i.6.f R h g h j.k K ( . . . . . . .   ",
"  . . . . . . . . . X ++2+] ] ] ] ] P ] j ( . . . . . . . . .   ",
"  . . . . . . . . . . . . . . . . . . . . . . . . . . . . . .   ",
"                                                                ",
"                                                                "};
EOF

$lightGreenLED = <<'EOF';
/* XPM */
static char* light_green_xpm[] = {
"32 32 226 2",
"  	c #FFFFFF",
". 	c #FEFFFE",
"+ 	c #FBFFFA",
"@ 	c #F3FFF2",
"# 	c #F1FFF0",
"$ 	c #F0FFEE",
"% 	c #EFFEED",
"& 	c #EDFEEB",
"* 	c #ECFEEA",
"= 	c #FCFFFB",
"- 	c #FDFFFD",
"; 	c #F2FFF1",
"> 	c #E4FEE2",
", 	c #CAFDC4",
"' 	c #C3FDBD",
") 	c #C0FDBA",
"! 	c #BDFDB6",
"~ 	c #B8FDB1",
"{ 	c #B1FDA9",
"] 	c #A9FCA0",
"^ 	c #A7FC9E",
"/ 	c #B9FDB2",
"( 	c #E0FEDD",
"_ 	c #EEFEEC",
": 	c #FCFFFC",
"< 	c #F5FFF4",
"[ 	c #E6FEE4",
"} 	c #CCFDC7",
"| 	c #C8FDC3",
"1 	c #C5FDBF",
"2 	c #C1FDBB",
"3 	c #BEFDB7",
"4 	c #B2FDAA",
"5 	c #AAFCA1",
"6 	c #A1FC98",
"7 	c #9AFC8F",
"8 	c #95FC8B",
"9 	c #99FC8F",
"0 	c #ADFCA4",
"a 	c #DEFEDB",
"b 	c #FAFFF9",
"c 	c #EBFEE9",
"d 	c #D6FED2",
"e 	c #C9FDC3",
"f 	c #C6FDC1",
"g 	c #CDFDC8",
"h 	c #CFFDCA",
"i 	c #C2FDBC",
"j 	c #BBFDB4",
"k 	c #B3FDAB",
"l 	c #A0FC96",
"m 	c #95FC8A",
"n 	c #8AFB7E",
"o 	c #88FB7C",
"p 	c #98FC8E",
"q 	c #BFFDB9",
"r 	c #E8FEE5",
"s 	c #E7FEE5",
"t 	c #CEFDC9",
"u 	c #D4FED0",
"v 	c #D8FED4",
"w 	c #DAFED6",
"x 	c #D7FED4",
"y 	c #D1FECD",
"z 	c #CBFDC5",
"A 	c #AEFCA6",
"B 	c #A4FC9B",
"C 	c #8EFB82",
"D 	c #83FB77",
"E 	c #7CFB6E",
"F 	c #E5FEE3",
"G 	c #CBFDC6",
"H 	c #BCFDB5",
"I 	c #D5FED1",
"J 	c #DEFEDA",
"K 	c #E4FEE1",
"L 	c #E1FEDE",
"M 	c #A7FC9F",
"N 	c #9CFC92",
"O 	c #91FB86",
"P 	c #85FB79",
"Q 	c #7AFB6D",
"R 	c #72FB64",
"S 	c #7DFB70",
"T 	c #F0FFEF",
"U 	c #F7FFF6",
"V 	c #BAFDB3",
"W 	c #D0FDCB",
"X 	c #EAFEE8",
"Y 	c #B4FDAD",
"Z 	c #9EFC94",
"` 	c #93FC88",
" .	c #87FB7B",
"..	c #70FA62",
"+.	c #6CFA5D",
"@.	c #DDFED9",
"#.	c #F6FFF5",
"$.	c #E3FEE0",
"%.	c #B5FDAE",
"&.	c #9FFC95",
"*.	c #94FC89",
"=.	c #7CFB6F",
"-.	c #66FA57",
";.	c #67FA58",
">.	c #EAFEE7",
",.	c #F6FFF6",
"'.	c #AFFCA7",
").	c #65FA56",
"!.	c #5FFA4F",
"~.	c #F9FFF8",
"{.	c #ACFCA3",
"].	c #B7FDB0",
"^.	c #6FFA60",
"/.	c #64FA54",
"(.	c #5AFA4A",
"_.	c #5DFA4C",
":.	c #8FFB84",
"<.	c #A5FC9C",
"[.	c #C5FDC0",
"}.	c #82FB76",
"|.	c #77FB6A",
"1.	c #6DFA5E",
"2.	c #62FA52",
"3.	c #57FA46",
"4.	c #52F940",
"5.	c #A3FC9A",
"6.	c #B6FDAF",
"7.	c #BFFDB8",
"8.	c #89FB7E",
"9.	c #7FFB72",
"0.	c #74FB66",
"a.	c #6AFA5B",
"b.	c #5EFA4E",
"c.	c #53FA42",
"d.	c #4FF93D",
"e.	c #78FB6B",
"f.	c #DBFED7",
"g.	c #9BFC90",
"h.	c #A6FC9D",
"i.	c #C4FDBE",
"j.	c #ABFCA2",
"k.	c #A2FC99",
"l.	c #7BFB6D",
"m.	c #5BFA4A",
"n.	c #51F93F",
"o.	c #4CF93A",
"p.	c #D9FED5",
"q.	c #8DFB81",
"r.	c #B3FDAC",
"s.	c #92FC87",
"t.	c #88FB7D",
"u.	c #75FB67",
"v.	c #6BFA5C",
"w.	c #61FA51",
"x.	c #48F936",
"y.	c #82FB75",
"z.	c #70FA61",
"A.	c #5CFA4B",
"B.	c #44F931",
"C.	c #71FA63",
"D.	c #8CFB80",
"E.	c #A1FC97",
"F.	c #96FC8C",
"G.	c #79FB6C",
"H.	c #68FA59",
"I.	c #56FA45",
"J.	c #42F92F",
"K.	c #3FF92C",
"L.	c #6EFA5F",
"M.	c #E2FEDF",
"N.	c #84FB78",
"O.	c #8BFB7F",
"P.	c #86FB7A",
"Q.	c #58FA48",
"R.	c #46F934",
"S.	c #3DF929",
"T.	c #3AF927",
"U.	c #6AFA5C",
"V.	c #81FB74",
"W.	c #76FB68",
"X.	c #69FA5A",
"Y.	c #59FA49",
"Z.	c #3FF92B",
"`.	c #37F923",
" +	c #76FB69",
".+	c #73FB65",
"++	c #7EFB71",
"@+	c #49F937",
"#+	c #40F92D",
"$+	c #38F924",
"%+	c #35F921",
"&+	c #60FA50",
"*+	c #64FA55",
"=+	c #50F93E",
"-+	c #39F925",
";+	c #32F81D",
">+	c #3BF928",
",+	c #58FA47",
"'+	c #52F941",
")+	c #33F81F",
"!+	c #54FA43",
"~+	c #55FA44",
"{+	c #4DF93B",
"]+	c #43F930",
"^+	c #36F922",
"/+	c #30F81B",
"(+	c #2FF81A",
"_+	c #4AF938",
":+	c #4EF93C",
"<+	c #3EF92A",
"[+	c #2EF819",
"}+	c #D2FECE",
"|+	c #97FC8D",
"1+	c #45F932",
"2+	c #45F933",
"3+	c #41F92E",
"4+	c #3CF928",
"5+	c #34F920",
"6+	c #2DF817",
"7+	c #5EFA4D",
"8+	c #F8FFF7",
"9+	c #31F81C",
"0+	c #4CF939",
"a+	c #F4FFF3",
"b+	c #4BF939",
"c+	c #DCFED8",
"                                                                ",
"                                                                ",
"  . . . . . . . . . + @ # # # # $ % & * # = - . . . . . . . .   ",
"  . . . . . . . + ; > , ' ' ) ! ~ { ] ^ / ( _ + . . . . . . .   ",
"  . . . . . : < [ } | 1 ' 2 3 / 4 5 6 7 8 9 0 a @ : . . . . .   ",
"  . . . . b c d e f , g h } e i j k ] l m n o p q r = . . . .   ",
"  . . . b s , i f t u v w x y z i / A B 9 C D E D ] F = . . .   ",
"  . . : _ G H 2 G I J K > L w y | ! k M N O P Q R S k T - . .   ",
"  . - U a H V 1 W w > & $ X L d z q Y ] Z `  .E ..+.n x b . .   ",
"  . = s j 4 V f y @.r # #._ $.x } 2 %.5 &.*.o =...-.;.m >.- .   ",
"  - ,.v '.A / 1 W w > & $ X L d z q Y ] Z `  .E ..).!.=.I ~..   ",
"  = > { B {.].2 G I J K > L w y | ! k M N O P Q ^./.(._.:.>.-   ",
"  + a <.Z M 4 H [.t u v w x y z i / A B 9 C }.|.1.2.3.4.Q K -   ",
"  + @.l 9 5.0 6.7.1 z g h } e i j k ] l m 8.9.0.a.b.c.d.e.K -   ",
"  + f.g.` N h.A %.j ) ' i.i 7./ k j.k.9 :.P l...).m.n.o.|.K -   ",
"  b p.8 q.8 Z h.{.{ %.].~ 6.r.'.] k.g.s.t.9.u.v.w.3.o.x.u.$.-   ",
"  ~.x :.P q.8 N k.^ 5 {.0 {.] <.l 9 s.n y.e.z.).A.4.x.B.C.$.-   ",
"  ~.d n S P D.s.p N &.E.6 E.Z g.F.:.t.y.G.C.H.!.I.o.J.K.L.M.-   ",
"  ~.u N.u.E y.o q.O *.8 F.8 ` :.O.P.9.e.C.a.w.Q.d.R.S.T.U.L -   ",
"  b v D...R e.S }.P.o 8.n 8. .N.V.E W.z.X.w.Y.n.x.Z.`.Z.9.[ -   ",
"  - ; i  +X.L..+|.l.=.++9.++E G.W.C.+.-.!.Q.n.@+#+$+%+&+g ~..   ",
"  . ~.I y./.*+X.1.z.C..+.+.+..L.v.;.2._.3.=+x.#+-+;+>+E F - .   ",
"  . - # q ..A.b.2.*+-.H.H.H.-./.&+_.,+'+o.R.K.$+;+)+).t ~.. .   ",
"  . . b @.s.b.!+3.Y.m.A._.A.m.Q.~+4.{+@+]+S.^+/+(+_+g.* - . .   ",
"  . . - < g }.(.:+:+=+n.4.n.d.o._+R.]+<+-+)+[+/+@+t.@.+ . . .   ",
"  . . . - < }+|+2.@+1+2+2+2+B.3+Z.4+$+5+(+6+%+7+k.a ~.. . . .   ",
"  . . . . - 8+s V z.!+3+>+T.-+`.5+9+[+6+5+0+ +} % = . . . . .   ",
"  . . . . . . - a+J ~ z.3.!+'+n.:+o.b+'+E e K ~.. . . . . . .   ",
"  . . . . . . . . : a+M.c+f.f.f.w w p.c+[ 8+- . . . . . . . .   ",
"  . . . . . . . . . . - - - - - - - - - - . . . . . . . . . .   ",
"                                                                ",
"                                                                "};
EOF

$lightRedLED = <<'EOF';
/* XPM */
static char *light_red_xpm[] = {
"32 32 227 2",
"  	c #FFFFFF",
". 	c #FFFEFE",
"+ 	c #FFFAFA",
"@ 	c #FFF1F1",
"# 	c #FFEFEF",
"$ 	c #FFEDED",
"% 	c #FFECEC",
"& 	c #FFEAEA",
"* 	c #FFE9E9",
"= 	c #FFFBFB",
"- 	c #FFFDFD",
"; 	c #FFF0F0",
"> 	c #FFE0E0",
", 	c #FFC1C1",
"' 	c #FFB9B9",
") 	c #FFB6B6",
"! 	c #FFB2B2",
"~ 	c #FFACAC",
"{ 	c #FFA4A4",
"] 	c #FF9B9B",
"^ 	c #FF9898",
"/ 	c #FFADAD",
"( 	c #FFDBDB",
"_ 	c #FFEBEB",
": 	c #FFFCFC",
"< 	c #FFF3F3",
"[ 	c #FFE2E2",
"} 	c #FFC4C4",
"| 	c #FFBFBF",
"1 	c #FFBBBB",
"2 	c #FFB7B7",
"3 	c #FFB3B3",
"4 	c #FFA5A5",
"5 	c #FF9C9C",
"6 	c #FF9292",
"7 	c #FF8989",
"8 	c #FF8484",
"9 	c #FF8888",
"0 	c #FF9F9F",
"a 	c #FFD9D9",
"b 	c #FFF9F9",
"c 	c #FFE8E8",
"d 	c #FFCFCF",
"e 	c #FFC0C0",
"f 	c #FFBDBD",
"g 	c #FFC5C5",
"h 	c #FFC7C7",
"i 	c #FFB8B8",
"j 	c #FFB0B0",
"k 	c #FFA6A6",
"l 	c #FF9090",
"m 	c #FF8383",
"n 	c #FF7777",
"o 	c #FF7474",
"p 	c #FF8787",
"q 	c #FFB5B5",
"r 	c #FFE4E4",
"s 	c #FFE3E3",
"t 	c #FFC6C6",
"u 	c #FFCDCD",
"v 	c #FFD2D2",
"w 	c #FFD4D4",
"x 	c #FFD1D1",
"y 	c #FFCACA",
"z 	c #FFC2C2",
"A 	c #FFA1A1",
"B 	c #FF9595",
"C 	c #FF7B7B",
"D 	c #FF6F6F",
"E 	c #FF6666",
"F 	c #FFE1E1",
"G 	c #FFC3C3",
"H 	c #FFB1B1",
"I 	c #FFCECE",
"J 	c #FFD8D8",
"K 	c #FFDFDF",
"L 	c #FFDCDC",
"M 	c #FF9999",
"N 	c #FF8C8C",
"O 	c #FF7F7F",
"P 	c #FF7171",
"Q 	c #FF6464",
"R 	c #FF5B5B",
"S 	c #FF6868",
"T 	c #FFEEEE",
"U 	c #FFF6F6",
"V 	c #FFAFAF",
"W 	c #FFC8C8",
"X 	c #FFE7E7",
"Y 	c #FFA8A8",
"Z 	c #FF8E8E",
"` 	c #FF8181",
" .	c #FF7373",
"..	c #FF5959",
"+.	c #FF5454",
"@.	c #FFD7D7",
"#.	c #FFF4F4",
"$.	c #FFDEDE",
"%.	c #FFA9A9",
"&.	c #FF8F8F",
"*.	c #FF8282",
"=.	c #FF6767",
"-.	c #FF4D4D",
";.	c #FF4E4E",
">.	c #FFE6E6",
",.	c #FFF5F5",
"'.	c #FFA2A2",
").	c #FFAEAE",
"!.	c #FF4C4C",
"~.	c #FF4545",
"{.	c #FFF8F8",
"].	c #FF9E9E",
"^.	c #FFABAB",
"/.	c #FF5757",
"(.	c #FF4A4A",
"_.	c #FF3F3F",
":.	c #FF4242",
"<.	c #FF7D7D",
"[.	c #FF9696",
"}.	c #FFBCBC",
"|.	c #FF6E6E",
"1.	c #FF6161",
"2.	c #FF5555",
"3.	c #FF4848",
"4.	c #FF3B3B",
"5.	c #FF3535",
"6.	c #FF9494",
"7.	c #FFAAAA",
"8.	c #FFB4B4",
"9.	c #FF7676",
"0.	c #FF6A6A",
"a.	c #FF5D5D",
"b.	c #FF5151",
"c.	c #FF4444",
"d.	c #FF3737",
"e.	c #FF3232",
"f.	c #FF6262",
"g.	c #FFD5D5",
"h.	c #FF8A8A",
"i.	c #FF9797",
"j.	c #FFBABA",
"k.	c #FF9D9D",
"l.	c #FF9393",
"m.	c #FF6565",
"n.	c #FF4040",
"o.	c #FF3434",
"p.	c #FF2F2F",
"q.	c #FFD3D3",
"r.	c #FF7A7A",
"s.	c #FFA7A7",
"t.	c #FF8080",
"u.	c #FF7575",
"v.	c #FF5E5E",
"w.	c #FF5353",
"x.	c #FF4747",
"y.	c #FF2A2A",
"z.	c #FF6D6D",
"A.	c #FF5858",
"B.	c #FF4141",
"C.	c #FF2525",
"D.	c #FF5A5A",
"E.	c #FF7979",
"F.	c #FF9191",
"G.	c #FF8585",
"H.	c #FF6363",
"I.	c #FF4F4F",
"J.	c #FF3A3A",
"K.	c #FF2323",
"L.	c #FF2020",
"M.	c #FF5656",
"N.	c #FFDDDD",
"O.	c #FF7070",
"P.	c #FF7878",
"Q.	c #FF7272",
"R.	c #FF3D3D",
"S.	c #FF2828",
"T.	c #FF1D1D",
"U.	c #FF1A1A",
"V.	c #FF5252",
"W.	c #FF6C6C",
"X.	c #FF5F5F",
"Y.	c #FF5050",
"Z.	c #FF3E3E",
"`.	c #FF1F1F",
" +	c #FF1616",
".+	c #FF6060",
"++	c #FF5C5C",
"@+	c #FF6969",
"#+	c #FF2B2B",
"$+	c #FF2121",
"%+	c #FF1717",
"&+	c #FF1414",
"*+	c #FF4646",
"=+	c #FF4B4B",
"-+	c #FF3333",
";+	c #FF1818",
">+	c #FF1010",
",+	c #FF1B1B",
"'+	c #FF3C3C",
")+	c #FF3636",
"!+	c #FF1212",
"~+	c #FF3838",
"{+	c #FF3939",
"]+	c #FF3030",
"^+	c #FF2424",
"/+	c #FF1515",
"(+	c #FF0E0E",
"_+	c #FF0D0D",
":+	c #FF2C2C",
"<+	c #FF3131",
"[+	c #FF1E1E",
"}+	c #FF0C0C",
"|+	c #FFCBCB",
"1+	c #FF8686",
"2+	c #FF2626",
"3+	c #FF2727",
"4+	c #FF2222",
"5+	c #FF1C1C",
"6+	c #FF1313",
"7+	c #FF0A0A",
"8+	c #FF4343",
"9+	c #FFF7F7",
"0+	c #FF0F0F",
"a+	c #FF2E2E",
"b+	c #FFF2F2",
"c+	c #FF2D2D",
"d+	c #FFD6D6",
"                                                                ",
"                                                                ",
"  . . . . . . . . . + @ # # # # $ % & * # = - . . . . . . . .   ",
"  . . . . . . . + ; > , ' ' ) ! ~ { ] ^ / ( _ + . . . . . . .   ",
"  . . . . . : < [ } | 1 ' 2 3 / 4 5 6 7 8 9 0 a @ : . . . . .   ",
"  . . . . b c d e f , g h } e i j k ] l m n o p q r = . . . .   ",
"  . . . b s , i f t u v w x y z i / A B 9 C D E D ] F = . . .   ",
"  . . : _ G H 2 G I J K > L w y | ! k M N O P Q R S k T - . .   ",
"  . - U a H V 1 W w > & $ X L d z q Y ] Z `  .E ..+.n x b . .   ",
"  . = s j 4 V f y @.r # #._ $.x } 2 %.5 &.*.o =...-.;.m >.- .   ",
"  - ,.v '.A ).1 W w > & $ X L d z q Y ] Z `  .E ..!.~.=.I {..   ",
"  = > { B ].^.2 G I J K > L w y | ! k M N O P Q /.(._.:.<.>.-   ",
"  + a [.Z M 4 H }.t u v w x y z i / A B 9 C |.1.2.3.4.5.Q K -   ",
"  + @.l 9 6.0 7.8.1 z g h } e i j k ] l m 9.0.a.b.c.d.e.f.K -   ",
"  + g.h.` N i.A %.j ) ' j.i 8.).k k.l.9 <.P m...!.n.o.p.1.K -   ",
"  b q.8 r.8 Z i.].{ %.^.~ 7.s.'.] l.h.t.u.0.v.w.x.4.p.y.v.$.-   ",
"  {.x <.P r.8 N l.^ 5 ].0 ].] [.l 9 t.n z.f.A.!.B.5.y.C.D.$.-   ",
"  {.d n S P E.t.p N &.F.6 F.Z h.G.<.u.z.H.D.I.~.J.p.K.L.M.N.-   ",
"  {.u O.v.E z.o r.O *.8 G.8 ` <.P.Q.0.f.D.b.x.R.e.S.T.U.V.L -   ",
"  b v E...R f.S |.Q.o 9.n 9. .O.W.E X.A.Y.x.Z.o.y.`. +`.0.[ -   ",
"  - ; i .+Y.M.++1.m.=.@+0.@+E H.X.D.+.-.~.R.o.#+$+%+&+*+g {..   ",
"  . {.I z.(.=+Y.2.A.D.++++++..M.w.;.3.:.4.-+y.$+;+>+,+E F - .   ",
"  . - # q ..B.c.3.=+-.I.I.I.-.(.*+:.'+)+p.S.L.%+>+!+!.t {.. .   ",
"  . . b @.t.c.~+4.Z.n.B.:.B.n.R.{+5.]+#+^+T./+(+_+:+h.* - . .   ",
"  . . - < g |._.<+<+-+o.5.o.e.p.:+S.^+[+;+!+}+(+#+u.@.+ . . .   ",
"  . . . - < |+1+3.#+2+3+3+3+C.4+`.5+%+6+_+7+&+8+l.a {.. . . .   ",
"  . . . . - 9+s V A.~+4+,+U.;+ +6+0+}+7+6+a+.+} % = . . . . .   ",
"  . . . . . . - b+J ~ A.4.~+)+o.<+p.c+)+E e K {.. . . . . . .   ",
"  . . . . . . . . : b+N.d+g.g.g.w w q.d+[ 9+- . . . . . . . .   ",
"  . . . . . . . . . . - - - - - - - - - - . . . . . . . . . .   ",
"                                                                ",
"                                                                "};
EOF

$darkRedLED = <<'EOF';
/* XPM */
static char* dark_red_xpm[] = {
"32 32 233 2",
"       c #FFFFFF",
".      c #FEFFFE",
"+ 	c #FEFCFC",
"@ 	c #FDF6F6",
"# 	c #FCF5F5",
"$ 	c #FCF3F3",
"% 	c #FCF2F2",
"& 	c #FBF2F2",
"* 	c #FBF1F1",
"= 	c #F9EBEB",
"- 	c #F4D5D5",
"; 	c #F2D1D1",
"> 	c #F2D0D0",
", 	c #F2CECE",
"' 	c #F1CCCC",
") 	c #F0C7C7",
"! 	c #EEC3C3",
"~ 	c #EDBDBD",
"{ 	c #ECBBBB",
"] 	c #F0C9C9",
"^ 	c #F9E7E7",
"/ 	c #FEFDFD",
"( 	c #FDF7F7",
"_ 	c #FAEBEB",
": 	c #F4D8D8",
"< 	c #F3D2D2",
"[ 	c #F1CACA",
"} 	c #F0C8C8",
"| 	c #EFC7C7",
"1 	c #EFC4C4",
"2 	c #EEBFBF",
"3 	c #ECB9B9",
"4 	c #EAB3B3",
"5 	c #E8ABAB",
"6 	c #E6A4A4",
"7 	c #E9AEAE",
"8 	c #EEC0C0",
"9 	c #F8E7E7",
"0 	c #FEFBFB",
"a 	c #FBEFEF",
"b 	c #F6DFDF",
"c 	c #F3D3D3",
"d 	c #F2CFCF",
"e 	c #F1CBCB",
"f 	c #EFC6C6",
"g 	c #EBB8B8",
"h 	c #E9AFAF",
"i 	c #E7A6A6",
"j 	c #E49E9E",
"k 	c #E29797",
"l 	c #E39797",
"m 	c #E9ADAD",
"n 	c #FAEEEE",
"o 	c #FEFAFA",
"p 	c #FAECEC",
"q 	c #F3D5D5",
"r 	c #F4D6D6",
"s 	c #F5DADA",
"t 	c #F5DBDB",
"u 	c #F4D9D9",
"v 	c #F3D4D4",
"w 	c #F1CECE",
"x 	c #EBB4B4",
"y 	c #E5A1A1",
"z 	c #E08E8E",
"A 	c #DF8C8C",
"B 	c #E39898",
"C 	c #EDBCBC",
"D 	c #F4D7D7",
"E 	c #F7E4E4",
"F 	c #F8E5E5",
"G 	c #F7E2E2",
"H 	c #F5DCDC",
"I 	c #EEC2C2",
"J 	c #ECB8B8",
"K 	c #E6A5A5",
"L 	c #E39B9B",
"M 	c #E09090",
"N 	c #DE8686",
"O 	c #DC8181",
"P 	c #E19090",
"Q 	c #EFC3C3",
"R 	c #FFFDFD",
"S 	c #FDF9F9",
"T 	c #F8E4E4",
"U 	c #EFC5C5",
"V 	c #EEC1C1",
"W 	c #F0CACA",
"X 	c #F6DCDC",
"Y 	c #F8E6E6",
"Z 	c #FAEDED",
"` 	c #E9B1B1",
" .	c #E49C9C",
"..	c #E19191",
"+.	c #DE8787",
"@.	c #DB7D7D",
"#.	c #DB7B7B",
"$.	c #E49B9B",
"%.	c #F6E0E0",
"&.	c #ECBABA",
"*.	c #F9E9E9",
"=.	c #ECBCBC",
"-.	c #EAB1B1",
";.	c #E7A7A7",
">.	c #E49D9D",
",.	c #E19292",
"'.	c #DE8888",
").	c #D97474",
"!.	c #DA7A7A",
"~.	c #FAEFEF",
"{.	c #FDF8F8",
"].	c #EBB5B5",
"^.	c #F6DEDE",
"/.	c #D97373",
"(.	c #D76E6E",
"_.	c #E08C8C",
":.	c #EAB4B4",
"<.	c #F5D9D9",
"[.	c #F7E1E1",
"}.	c #F1CDCD",
"|.	c #E9B0B0",
"1.	c #E6A6A6",
"2.	c #DB7C7C",
"3.	c #D87272",
"4.	c #D66969",
"5.	c #D86F6F",
"6.	c #EBB6B6",
"7.	c #E8ADAD",
"8.	c #E6A2A2",
"9.	c #DD8484",
"0.	c #D87070",
"a.	c #D56666",
"b.	c #D46262",
"c.	c #DF8989",
"d.	c #F7E0E0",
"e.	c #E7A9A9",
"f.	c #E8ACAC",
"g.	c #E49F9F",
"h.	c #E29494",
"i.	c #DF8B8B",
"j.	c #D97777",
"k.	c #D76D6D",
"l.	c #D46363",
"m.	c #D35E5E",
"n.	c #EBB7B7",
"o.	c #E39A9A",
"p.	c #DB7E7E",
"q.	c #D66A6A",
"r.	c #D36060",
"s.	c #D25D5D",
"t.	c #DD8383",
"u.	c #E59F9F",
"v.	c #E5A0A0",
"w.	c #E7A8A8",
"x.	c #EDBFBF",
"y.	c #E19494",
"z.	c #DC8282",
"A.	c #DA7979",
"B.	c #D15959",
"C.	c #FDFAFA",
"D.	c #EAB2B2",
"E.	c #E8AAAA",
"F.	c #E29696",
"G.	c #E08D8D",
"H.	c #DE8585",
"I.	c #D66B6B",
"J.	c #D46161",
"K.	c #D05555",
"L.	c #DC7E7E",
"M.	c #E29595",
"N.	c #DF8A8A",
"O.	c #E7AAAA",
"P.	c #D56565",
"Q.	c #D05353",
"R.	c #CF5151",
"S.	c #DD8282",
"T.	c #DE8989",
"U.	c #E08F8F",
"V.	c #E39999",
"W.	c #DC7F7F",
"X.	c #DA7878",
"Y.	c #D56868",
"Z.	c #D15757",
"`.	c #CE4F4F",
" +	c #CE4C4C",
".+	c #DC8080",
"++	c #DA7777",
"@+	c #CD4A4A",
"#+	c #CF4F4F",
"$+	c #DD8585",
"%+	c #DB7A7A",
"&+	c #D97575",
"*+	c #D66868",
"=+	c #D25A5A",
"-+	c #D05252",
";+	c #CD4B4B",
">+	c #CD4747",
",+	c #FCF6F6",
"'+	c #D97676",
")+	c #D87171",
"!+	c #D76C6C",
"~+	c #D56767",
"{+	c #D36161",
"]+	c #D15A5A",
"^+	c #CE4B4B",
"/+	c #CC4444",
"(+	c #F7E3E3",
"_+	c #D66C6C",
":+	c #D15858",
"<+	c #CF5252",
"[+	c #CC4545",
"}+	c #CB4444",
"|+	c #CB4242",
"1+	c #D15656",
"2+	c #F9E8E8",
"3+	c #D35F5F",
"4+	c #D25C5C",
"5+	c #CF5050",
"6+	c #CC4747",
"7+	c #D05454",
"8+	c #D05656",
"9+	c #CB4343",
"0+	c #CA4040",
"a+	c #CC4646",
"b+	c #D46464",
"c+	c #F2D2D2",
"d+	c #FCF4F4",
"e+	c #CE4E4E",
"f+	c #CE4D4D",
"g+	c #CD4848",
"h+	c #CB4040",
"i+	c #D25B5B",
"j+	c #FBF0F0",
"                                                                ",
"                                                                ",
"  . . . . . . . . . + @ # # # # $ % & * # + . . . . . . . . .   ",
"  . . . . . . . + # = - ; > , ' ) ! ~ { ] ^ % + . . . . . . .   ",
"  . . . . . / ( _ : < [ } | 1 2 3 4 5 6 6 7 8 9 @ / . . . . .   ",
"  . . . . 0 a b c ' , > ; d e f 2 g h i j k l m d n + . . . .   ",
"  . . . o p q e ' ; r s t u v w f ~ x 5 y l z A B C _ + . . .   ",
"  . . / & - 1 ) d D b E F G H v e I J h K L M N O P Q $ R . .   ",
"  . R S T U V W c X Y Z a = G u d U { ` i  ...+.@.#.$.%.0 . .   ",
"  . + _ ) &.8 [ q b *.& # a F t ; | =.-.;.>.,.'.@.).!.5 ~.R .   ",
"  R {.b J ].2 W v ^.^ a & Z E s > f =.-.;. .,.'.@./.(._.X o .   ",
"  + *.2 7 :.~ ) ; <.[.^ *.Y b r }.Q 3 |.1.$.P N 2.3.4.5.i ~..   ",
"  0 G h ;.|.3 ! e c <.^.b X D ; } 2 6.7.8.B z 9.!.0.a.b.c.^ R   ",
"  0 d.e.8.f.x ~ U e ; v q c d ] ! &.-.e.g.h.i.O j.k.l.m.9.Y R   ",
"  0 b 6 j i h n.~ ! ) [ [ W f V { :.5 8.o.P +.p./.q.r.s.t.Y R   ",
"  o X u.l v.w.h x 3 ~ 2 8 x.C g 4 f.K  .y.i.z.A.5.a.s.B.z.Y R   ",
"  C.t L ..B v.i f.|.:.].].x D.7 E.6 >.F.G.H.@./.I.J.B.K.L.F R   ",
"  C.<.M.N.P l j 8.i O.5 5 E.e.K y $.M.z N L.j.(.P.s.Q.R.#.T R   ",
"  C.: M S.T.U.h.V. .u.y y v.g.$.k ,._.N W.X.0.Y.r.Z.`. +X.E R   ",
"  o t h.p..+H.i.U.,.h.F.k M.y...G.T.9.p.++0.4.J.B.R.@+#+N ^ R   ",
"  R a ~ O ++@.O $+'.N.A A i.c.+.9.W.%+&+5.*+J.=+-+;+>+4.! ,+.   ",
"  . S q N.3./.++#.p..+z.z.O W.@.!.'+)+!+~+{+]+-+^+/+^+S.(+/ .   ",
"  . / n 3 %+_+(.)+).'+++++j.&+/.0.!+Y.l.m.:+<+;+/+[+_+! ,+. .   ",
"  . . C.t M._+P.~+q._+!+!+!+I.4.a.l.m.=+K.#+@+}+|+1+V.2+/ . .   ",
"  . . R % | +.Y.3+3+J.b.b.J.{+m.4+B.K.5+^+6+|+|+7+'.c {.. . .   ",
"  . . . / ~.) h._+]+Z.:+:+Z.8+7+<+`.^+>+9+0+a+b+L c+d+R . . .   ",
"  . . . . / % t |.).r.-+e+f+ +@+g+[+|+h+[+8+++C [.@ R . . . .   ",
"  . . . . . R C._ }.f./.{+m.s.i+]+Z.8+i+#.g c+* 0 R . . . . .   ",
"  . . . . . . . / ( _ ; ) f f U 1 1 Q f q j+S R . . . . . . .   ",
"  . . . . . . . . . / S ( ( ( ( ( ( ( ( C.R . . . . . . . . .   ",
"                                                                ",
"                                                                "};
EOF
