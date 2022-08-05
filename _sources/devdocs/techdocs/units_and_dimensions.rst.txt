Handling of units and dimensions
================================

The Hydra server implements checks that make sure that the units of a dataset
assigned to an attribute are consistent with the physical dimension asked for by
the attribute. This requires some conventions about how physical dimensions are
denoted in the respective fields of the database. Also, a standard way of
describing physical units needs to be defined. This document describes these
conventions and provides a controlled vocabulary for both, dimensions and units.

.. contents:: Table of Contents
   :local:

Definitions
-----------

**Dimension**
    In this document a dimension is the physical dimension of a physical
    quantity. A dimension is independent of the units used to describe a
    physical quantity.

**Unit**
    A unit defines the magnitude of a physical quantity. A unit is defined by
    convention and refers to a system of measurement, such as `SI
    <http://en.wikipedia.org/wiki/International_System_of_Units>`_.

Dimensions
----------

Basic concepts
~~~~~~~~~~~~~~

There are two basic ways of defining physical dimensions. 

#. Define a dimension as mathematical expression based on the seven fundamental
   quantities:

   ======================= ===============
   Base quantity           Symbol
   ----------------------- ---------------
   *Length*                :math:`L`
   *Mass*                  :math:`M`
   *Time*                  :math:`T`
   *Electric current*      :math:`I`
   *Temperature*           :math:`\Theta`
   *Amount of substance*   :math:`N`
   *Luminous intensity*    :math:`J`
   ======================= ===============

   All derived quantities can be expressed based on these fundamental
   quantities. For example *Energy* would be written as :math:`M L^{2} T^{-2}`.

#. Define a dimension using a keyword. This will allow to set fundamental and
   derived quantities using a name defined by a controlled vocabulary. 

In Hydra the second definition is implemented since expressing all the derived
quantities based on the fundamental ones is rather complicated, even for
quantities that are used frequently (such as energy, power, etc.).

List of dimension keywords
~~~~~~~~~~~~~~~~~~~~~~~~~~

  ======================== =
  ``Length``
  ``Mass``
  ``Time``
  ``Temperature``
  ``Area``
  ``Volume``
  ``Angle``
  ``Speed``
  ``Energy``
  ``Force``
  ``Power``
  ``Pressure``
  ``Volumetric flow rate``
  ``Monetary value``
  ``Unit price (volume)``
  ``Unit price (mass)``
  ``Energy price``
  ``Dimensionless``
  ======================== =


Units
-----

============================ ============ ================ ============= ==========================
Unit                         Abbr.        Linear factor    Constant fac. Description
============================ ============ ================ ============= ==========================
**Dimensionless**            
No unit                      ``-``        1.0              0.0           Dimensionless parameter
                                                                         without units
Percent                      %            0.01             0.0           
**Energy price**
US Dollars per joule         USD J^-1     1.0              0.0           Cost in US Dollars per
                                                                         Joule
US Dollars per kilojoule     USD kJ^-1    0.001            0.0           
US Dollars per kilowatt-hour USD kWh^-1   2.77777777e-07   0.0 
**Mass flow rate**
kilograms per second         kg s^-1      1.0              0.0
kilograms per minute         kg min^-1    0.0166666667     0.0
kilograms per hour           kg h^-1      0.000277777778   0.0
kilograms per day            kg day^-1    1.15740741e-05   0.0
kilograms per month          kg mon^-1    3.80265176e-07   0.0
kilograms per year           kg yr^-1     3.16887646e-08   0.0
grams per second             g s^-1       0.001            0.0
tonnes per second            kg s^-1      1000.0           0.0
tonnes per minute            kg min^-1    16.66666666667   0.0
tonnes per hour              kg h^-1      0.277777777778   0.0
tonnes per day               kg day^-1    1.15740741e-02   0.0
tonnes per month             kg mon^-1    3.80265176e-04   0.0
tonnes per year              kg yr^-1     3.16887646e-05   0.0
**Volumetric flow rate**
cubic metres per second      m^3 s^-1     1.0              0.0           SI unit for volumetric
                                                                         flow rate.
cubic metres per minute      m^3 min^-1   0.0166666667     0.0
cubic metres per hour        m^3 h^-1     0.000277777778   0.0
cubic metres per day         m^3 day^-1   1.15740741e-05   0.0
cubic metres per month       m^3 mon^-1   3.80265176e-07   0.0
cubic hectometres per second hm^3 s^-1    1000000.0        0.0           SI unit for volumetric
                                                                         flow rate.
cubic hectometres per minute hm^3 min^-1  16666.6667       0.0
cubic hectometres per hour   hm^3 h^-1    277.777778       0.0
cubic hectometres per day    hm^3 day^-1  11.5740741       0.0
cubic hectometres per month  hm^3 mon^-1  0.380265176      0.0
cubic foot per second        ft^3 s^-1    0.0283168466     0.0
cubic foot per minute        ft^3 min^-1  0.000471947443   0.0
cubic foot per hour          ft^3 h^-1    7.86579072e-06   0.0
cubic foot per day           ft^3 day^-1  3.2774128e-07    0.0
cubic foot per month         ft^3 mon^-1  1.07679106e-08   0.0
gallons per second           gal s^-1     0.00378541178    0.0
gallons per minute           gal min^-1   6.30901964e-05   0.0
gallons per hour             gal h^-1     1.05150327e-06   0.0
gallons per day              gal day^-1   4.38126364e-08   0.0
gallons per month            gal mon^-1   1.43946028e-09   0.0
acre-foot per second         ac-ft s^-1   1233.48184       0.0
acre-foot per minute         ac-ft min^-1 20.5580306       0.0
acre-foot per hour           ac-ft h^-1   0.342633844      0.0
acre-foot per day            ac-ft day^-1 0.0142764102     0.0
acre-foot per month          ac-ft mon^-1 0.000469050188   0.0
acre-inch per second         ac-in s^-1   102.790153       0.0
acre-inch per minute         ac-in min^-1 1.71316922       0.0
acre-inch per hour           ac-in h^-1   0.0285528203     0.0
acre-inch per day            ac-in day^-1 0.00118970085    0.0
acre-inch per month          ac-in mon^-1 3.90875157e-05   0.0
litre per second             l s^-1       0.001            0.0
litre per minute             l min^-1     1.66666667e-05   0.0
litre per hour               l h^-1       2.77777778e-07   0.0
litre per day                l day^-1     1.15740741e-08   0.0
litre per month              l mon^-1     3.80265176e-10   0.0
megalitre per second         Ml s^-1      1000             0.0
megalitre per minute         Ml min^-1    1.66666667       0.0
megalitre per hour           Ml h^-1      2.77777778e-01   0.0
megalitre per day            Ml day^-1    1.15740741e-02   0.0
megalitre per month          Ml mon^-1    3.80265176e-04   0.0

**Angle**
degree                       °            1.0              0.0           Is a measurement of plane
                                                                         angle, representing 1/360
                                                                         of a full rotation
grad or gon                  grd          0.9              0.0           One grad equals 9/10 of a
                                                                         degree or π/200 of a
                                                                         radian     
radian                       rad          57.29577951      0.0           1 rad=180/π 
minutes                      ``'``        0.0166666666     0.0           1°=1/60     
seconds                      ``''``       0.00027777777778 0.0           1°=1/3600   
**Temperature**
Celsius                      °C           1.0              273.15        The Celsius scale sets
                                                                         0.01 °C to be at the
                                                                         triple point of water and
                                                                         a degree Celsius to be
                                                                         1/273.16 of the difference
                                                                         in temperature between the
                                                                         triple point of water and
                                                                         absolute zero. Until 1954
                                                                         the scale was defined with
                                                                         the freezing point of
                                                                         water at 0 °C and the
                                                                         boiling point at 100 °C at
                                                                         standard atmospheric
                                                                         pressure.
Delisle                      °De          -0.666666666667  373.15        The Delisle scale is a
                                                                         temperature scale invented
                                                                         in 1732 by the French 
                                                                         astronomer Joseph-Nicolas
                                                                         Delisle (16881768). It is
                                                                         similar to that of Reaumur
Electronvolt                 eV           11605.0          0.0           In some fields, plasma
                                                                         physics in particular, the
                                                                         electronvolt (eV) is used
                                                                         as a unit of 'temperature'
Fahrenheit                   °F           0.555555555556   255.372222222 In this scale, the
                                                                         freezing point of water is
                                                                         32 degrees Fahrenheit 
                                                                         (written as 32 °F), and 
                                                                         the boiling point is 212 
                                                                         degrees, placing the 
                                                                         boiling and melting points
                                                                         of water 180 degrees 
                                                                         apart. Thus the unit of
                                                                         this scale, a degree 
                                                                         Fahrenheit, is 5/9ths of a
                                                                         kelvin (which is a degree
                                                                         Celsius), and negative 40
                                                                         degrees Fahrenheit is
                                                                         equal to negative 40
                                                                         degrees Celsius
Kelvin                       K            1.0              0.0           The kelvin, unit of 
                                                                         thermodynamic temperature
                                                                         is the fraction 1/273.16 
                                                                         of the thermodynamic 
                                                                         temperature of the triple
                                                                         point of water. [13th CGPM
                                                                         (1967), Resolution 4]
Rankine                      °Ra          0.555555555556   0.0           Like Kelvin, Rankine zero
                                                                         is absolute zero, but
                                                                         Fahrenheit degrees are 
                                                                         used. As a result, a 
                                                                         difference of 1°R is equal
                                                                         to a difference of 1°F, 
                                                                         but 0°R is 459.67°F
Réaumur                      °Ré          1.25             273.15        The freezing point of 
                                                                         water is 0 degrees
                                                                         Réaumur, the boiling point
                                                                         80 degrees Réaumur. Hence
                                                                         a degree Reaumur is 1.25
                                                                         degrees Celsius or
                                                                         kelvins. The Réaumur 
                                                                         temperature scale is also 
                                                                         known as the octogesimal 
                                                                         division (division 
                                                                         octogesimale)
Rømer                        °Rø          1.90476190476    258.864285714 Rømer is a disused 
                                                                         temperature scale named 
                                                                         after the Danish 
                                                                         astronomer Ole Christensen
                                                                         Rømer, who proposed it in
                                                                         1701
**Power**
BTU/hour                     BTU h^-1     0.29301067       0.0
BTU/minutes                  BTU min^-1   17.56863         0.0
BTU/seconds                  BTU s^-1     1055.056         0.0
calorie/seconds              cal s^-1     4.183076         0.0
gigawatt                     GW           1000000000.0     0.0
horsepower                   hp           745.699871582    0.0           The mechanical horsepower
                                                                         is originally proposed by
                                                                         James Watt in 1782.
watt                         W            1.0              0.0           One watt is one joule of
                                                                         energy per second
kilowatt                     kW           1000.0           0.0
megawatt                     MW           1000000.0        0.0
gigawatt                     GW           1000000000.0     0.0
volt-ampere                  VA           1.0              0.0           A volt-ampere in 
                                                                         electrical terms, means 
                                                                         the amount of apparent 
                                                                         power in an alternating 
                                                                         current circuit equal to 
                                                                         a current of one ampere 
                                                                         at an emf of one volt. It
                                                                         is dimensionally 
                                                                         equivalent to watts
**Area**
square metre                 m^2          1.0              0.0
square kilometre             km^2         1000000.0        0.0
are                          a            100.0            0.0 
acre                         ac           4046.8564224     0.0           International acre.
acre(US)                     ac (US)      4046.87261       0.0           United States survey acre.
hectare                      ha           10000.0          0.0           Commonly used for 
                                                                         measuring land area.
square yard                  yd^2         0.83612736       0.0
square foot                  ft^2         0.09290304       0.0
square inch                  in^2         0.00064516       0.0
square mile                  mi^2         2589988.11034    0.0
**Energy**
BTU(IT)                      BTU          1055.056         0.0           The British thermal unit
                                                                         (BTU or Btu) is a unit of
                                                                         energy used in the United
                                                                         States. In most other 
                                                                         areas, it has been 
                                                                         replaced by the SI unit of
                                                                         energy, the joule (J). A 
                                                                         Btu is defined as the 
                                                                         amount of heat required to
                                                                         raise the temperature of 
                                                                         one pound avoirdupois of 
                                                                         water by one degree 
                                                                         Fahrenheit. 143 Btu is 
                                                                         required to melt a pound 
                                                                         of ice. As is the case 
                                                                         with the calorie, several 
                                                                         different definitions of 
                                                                         the Btu exist, here ISO 
                                                                         BTU is used  1 ISO BTU = 
                                                                         1055.056 J
calorie(IT)                  cal          4.1868           0.0           The small calorie or gram
                                                                         calorie approximates the 
                                                                         energy needed to increase 
                                                                         the temperature of 1 g of 
                                                                         water by 1C. Here the 
                                                                         definition adopted by the 
                                                                         Fifth International 
                                                                         Conference on Properties 
                                                                         of Steam (London, July 
                                                                         1956) is used. 1 cal = 
                                                                         4.1868 J exactly.
Electronvolt                 eV           11605.0          0.0           In some fields, plasma 
                                                                         physics in particular, 
                                                                         the electronvolt (eV) is 
                                                                         used as a unit of 
                                                                         'temperature'
erg                          erg          1e-07            0.0           An erg is the unit of 
                                                                         energy and mechanical 
                                                                         work in the 
                                                                         centimetre-gram-second 
                                                                         (CGS) system of units. Its
                                                                         name is derived from the 
                                                                         Greek word meaning 'work'. 
                                                                         The erg is a quite small 
                                                                         unit, equal to a force of 
                                                                         one dyne exerted for a 
                                                                         distance of one centimetre
gigajoule                    GJ           1000000000.0     0.0
horsepower-hours             hph          2684520.0        0.0
joule                        J            1.0              0.0           The joule is a derived 
                                                                         unit defined as the work 
                                                                         done or energy required, 
                                                                         to exert a force of one 
                                                                         newton for a distance of 
                                                                         one metre, so the same 
                                                                         quantity may be referred 
                                                                         to as a newton metre or 
                                                                         newton-metre with the 
                                                                         symbol N·m. However, the
                                                                         newton metre is usually 
                                                                         used as a measure of 
                                                                         torque, not energy
kilojoule                    kJ           1000.0           0.0
kilocalorie                  kcal         4184.0           0.0
watt-hour                    Wh           3600.0           0.0           One watt-hour is 
                                                                         equivalent to one watt of 
                                                                         power used for one hour. 
                                                                         This is equivalent to 
                                                                         3,600 joules. For example,
                                                                         a sixty watt light bulb
                                                                         uses 60 watt-hours of
                                                                         energy every hour
kilowatt-hour                kWh          3600000.0        0.0
Megawatt-hour                MWh          3600000000.0     0.0
Gigawatt-hour                GWh          3.6e+12          0.0
megajoule                    MJ           1000000.0        0.0
**Volume**
barrel(oil)                  bbl          0.158987295      0.0           The standard oil barrel is
                                                                         used in the United States 
                                                                         for crude oil or other 
                                                                         petroleum products. 1 Oil 
                                                                         barrel = 42 US gallons
centilitre                   cl           1e-05            0.0
cubic centimetre             cm^3         1e-06            0.0
cubic decimetre              dm^3         0.001            0.0
cubic hectometre             hm^3         1000000.0        0.0
cubic foot                   ft^3         0.028316846592   0.0
cubic inch                   in^3         1.6387064e-05    0.0
cubic metre                  m^3          1.0              0.0
cubic millimetre             mm^3         1e-09            0.0
cubic yard                   yd^3         0.764554857984   0.0
decilitre                    dl           0.0001           0.0
fluid ounce(US)              fl oz        2.9574e-05       0.0
gallon, liquid(US)           gal          0.003785411784   0.0           US liquid gallon is 231 
                                                                         in^3 or 128 fl oz or 
                                                                         3.785411784 L.
litre                        L            0.001            0.0           A litre is defined as a 
                                                                         special name for a cubic 
                                                                         decimetre (1 L = 1 dm^3).
decilitre                    dl           0.0001           0.0
millilitre                   ml           1e-06            0.0
megalitre                    Ml           1000             0.0           A megalitre is a unit used
                                                                         in water management (1 Ml 
                                                                         = 10^3 m^3)
pint, liquid(US)             pt           0.000473176475   0.0
acre-foot                    ac-ft        1.23348184       0.0           An acre foot is the volume
                                                                         of water that covers one 
                                                                         acre in one foot. This 
                                                                         unit is popular among 
                                                                         irrigation people in the 
                                                                         US.
acre-inch                    ac-in        0.102790153      0.0           See acre-foot.
**Pressure**
atmosphere                   atm          101325.0         0.0           an atmosphere (symbol: 
                                                                         atm) or standard 
                                                                         atmosphere is a unit of 
                                                                         pressure roughly equal to
                                                                         the average atmospheric 
                                                                         pressure at sea level on 
                                                                         Earth. It is defined as 
                                                                         101.325 kPa and equal to 
                                                                         the pressure under 760 mm 
                                                                         of mercury
pascal                       Pa           1.0              0.0           The pascal is equivalent 
                                                                         to one newton per square 
                                                                         metre, and was used in SI 
                                                                         under that name before 
                                                                         the name pascal was 
                                                                         adopted by the 14th CGPM 
                                                                         in 1971. The same unit is
                                                                         also used for stress, 
                                                                         Young's modulus, and 
                                                                         tensile strength
bar                          bar          100000.0         0.0
hectopascal                  hPa          100.0            0.0
iches of water               inH2O        249.08891        0.0
inches of mercury            inHg         3386.388         0.0           Inches of mercury is a 
                                                                         non-SI unit for pressure. 
                                                                         It is defined as the 
                                                                         pressure exerted by a 
                                                                         column of mercury of 1 
                                                                         inch in height at 0 °C at 
                                                                         the standard acceleration 
                                                                         of gravity. 1 inHg = 
                                                                         3386.389 pascals at 0 °C.
kilopascal                   kPa          1000.0           0.0
metre of water               mH2O         9806.65          0.0
microbar                     µbar         0.1              0.0
milibar                      mbar         100.0            0.0
millimetre of mercury        mmHg         133.322          0.0
millimetre of water          mmH2O        9.80665          0.0
lbf/in^2                     psi          6894.76          0.0           The pound-force per square
                                                                         inch (symbol: lbf/in^2) is
                                                                         a non-SI unit of pressure 
                                                                         based on avoirdupois 
                                                                         units. In casual English 
                                                                         language use it is 
                                                                         rendered as 'pounds per 
                                                                         square inch', abbreviated 
                                                                         to psi with little 
                                                                         distinction between 'mass' 
                                                                         and 'force'
technical atmosphere         at           98066.5          0.0           A technical atmosphere is 
                                                                         a non-SI unit of pressure 
                                                                         equal to 1 kilogram-force 
                                                                         per square centimetre, 
                                                                         i.e. 98.066 5 kilopascals 
                                                                         (kPa) or about 0.96784
                                                                         standard atmospheres
torr                         torr         133.322          0.0           The torr (symbol: Torr) or
                                                                         millimetre of mercury 
                                                                         (mmHg) is a non-SI unit of
                                                                         pressure. It is the
                                                                         atmospheric pressure that 
                                                                         supports a column of 
                                                                         mercury 1 millimetre high
**Length**
angström                     Å            1e-10            0.0           Angstrom sometimes used 
                                                                         expressing the size of 
                                                                         atoms, and lengths of 
                                                                         chemical bonds and 
                                                                         visible-light spectra.
astronomical unit            AU           1.4959855e+11    0.0           Is a unit of length 
                                                                         approximately equal to the
                                                                         distance from the Earth to
                                                                         the Sun.
centimetre                   cm           0.01             0.0
decimetre                    dm           0.1              0.0
femtometre                   fm           1e-15            0.0
foot                         ft           0.3048           0.0           International foot. Foot 
                                                                         is a unit of length, in a
                                                                         number of different 
                                                                         systems, including English
                                                                         units, Imperial units, and
                                                                         United States customary 
                                                                         units. Its size can vary 
                                                                         from system to system, but 
                                                                         in each is around a 
                                                                         quarter to a third of a 
                                                                         metre. The most commonly
                                                                         used foot today is the
                                                                         international foot.
hectometre                   hm           100.0            0.0
inch                         in           0.0254           0.0           The international inch is
                                                                         defined to be precisely 
                                                                         25.4 mm
kilometre                    km           1000.0           0.0
light-year                   ly           9.460528405e+15  0.0           A light-year is the 
                                                                         distance that light 
                                                                         travels in a vacuum in one
                                                                         year. While there is no
                                                                         authoritative decision on 
                                                                         which year is used, the 
                                                                         International Astronomical
                                                                         Union (IAU) recommends the
                                                                         Julian year.
metre                        m            1.0              0.0           Is the fundamental unit of
                                                                         length in the 
                                                                         International System of 
                                                                         Units (SI). The metre is 
                                                                         defined as the length of 
                                                                         the path traveled by light
                                                                         in vacuum during a time
                                                                         interval of 1/299,792,458
                                                                         second.
micrometre                   µm           1e-06            0.0
mile                         mi           1609.344         0.0           The international mile is
                                                                         defined to be precisely 
                                                                         1760 international yards 
                                                                         (by definition, 0.9144 m 
                                                                         each) and is therefore 
                                                                         exactly 1609.344 metres.
mile(nautical)               nmi          1852.0           0.0           Corresponds approximately
                                                                         to one minute of latitude 
                                                                         along any meridian. It is 
                                                                         a non-SI unit used by 
                                                                         special interest groups 
                                                                         such as navigators in the
                                                                         shipping and aviation 
                                                                         industries. It is commonly
                                                                         used in international law
                                                                         and treaties, especially
                                                                         regarding the limits of 
                                                                         territorial waters. It 
                                                                         developed from the 
                                                                         geographical mile.
millimetre                   mm           0.001            0.0
nanometre                    nm           1e-09            0.0
parsec                       pc           3.0856776e+16    0.0           The name parsec stands for
                                                                         ''parallax of one second 
                                                                         of arc'', and one parsec 
                                                                         is defined to be the 
                                                                         distance from the Earth to
                                                                         a star that has a parallax
                                                                         of 1 arcsecond.
picometre                    pm           1e-12            0.0
yard                         yd           0.9144           0.0           The international yard is
                                                                         defined as 3 feet, 36 
                                                                         inches, or 1/1760 of a 
                                                                         mile, which is exactly 
                                                                         0.9144 metres.
**Mass**
carat                        carat        0.0002           0.0           The carat is a unit of 
                                                                         mass used for gems, and 
                                                                         equals 200 milligrams. The
                                                                         word derives from the 
                                                                         Greek keration (fruit of 
                                                                         the carob), via Arabic and
                                                                         Italian. Carob seeds were
                                                                         used as weights on 
                                                                         precision scales because 
                                                                         of their uniform weight. 
                                                                         In the distant past, 
                                                                         different countries each 
                                                                         had their own carat, 
                                                                         roughly equivalent to a 
                                                                         carob seed
gram                         g            0.001            0.0
kilogram                     kg           1.0              0.0           The kilogram is the unit 
                                                                         of mass equal to the mass
                                                                         of the international
                                                                         prototype of kilogram. 
                                                                         [1st CGPM (1889), 3rd CGPM
                                                                         (1901)]. It is the only SI
                                                                         unit that is still defined
                                                                         in relation to an artifact
                                                                         rather than to a 
                                                                         fundamental physical
                                                                         property that can be
                                                                         reproduced in different
                                                                         laboratories.
microgram                    µg           1e-09            0.0
milligram                    mg           1e-06            0.0
ounce                        oz           0.02835          0.0           International avoirdupois
                                                                         ounce (most common). The
                                                                         abbreviation ''oz'' comes 
                                                                         from the old Italian word
                                                                         ''onza'' (now spelled
                                                                         oncia), meaning ounce.
pound                        lbm          0.45359237       0.0           The pound is the name of a
                                                                         number of units of mass,
                                                                         all in the range of 300 to 
                                                                         600 grams. Most commonly,
                                                                         it refers to the
                                                                         avoirdupois pound (454 g),
                                                                         divided into 16
                                                                         avoirdupois ounces.
tonne                        t            1000.0           0.0
**Time**
day                          day          86400.0          0.0
hour                         h            3600.0           0.0
minute                       min          60.0             0.0
month                        mon          2629743.8328     0.0           Here: 1 month =  1/12
                                                                         year.              
                                                                         January = 31 days              
                                                                         February, 28 days, 29 in 
                                                                         leap years, or 30 on 
                                                                         certain occasions in 
                                                                         related calendars             
                                                                         March, 31 days             
                                                                         April, 30 days             
                                                                         May, 31 days             
                                                                         June, 30 days            
                                                                         July, 31 days
                                                                         August, 31 days   
                                                                         September, 30 days    
                                                                         October, 31 days  
                                                                         November, 30 days  
                                                                         December, 31 days
second                       s            1.0              0.0           The second is the SI base
                                                                         unit of time and is 
                                                                         defined as the duration of
                                                                         9 192 631 770 periods of 
                                                                         the radiation 
                                                                         corresponding to the 
                                                                         transition between the two
                                                                         hyperfine levels of the
                                                                         ground state of the 
                                                                         caesium-133 atom. This 
                                                                         definition refers to a 
                                                                         caesium atom at rest at a 
                                                                         temperature of 0 K
millisecond                  ms           0.001            0.0
microsecond                  μs           1e-06            0.0
nanosecond                   ns           1e-09            0.0
picosecond                   ps           1e-12            0.0
year                         yr           31556925.9936    0.0           Here: 1 year = 365.242199
                                                                         days.
**Force**
dyne                         dyn          1e-05            0.0           The dyne is a unit of force
                                                                         specified in the 
                                                                         centimetre-gram-second 
                                                                         (cgs) system of units. One
                                                                         dyne is equal to exactly
                                                                         10-5 newtons. The dyne can
                                                                         be defined as 'the force
                                                                         required to accelerate a
                                                                         mass of one gram at a rate 
                                                                         of one centimetre per
                                                                         second squared'
gram-force                   gf           0.00980665       0.0
joule/metre                  J m^-1       1.0              0.0
kg·m/s^2                     kg m s^-2    1.0              0.0           Same as 1 newton
kilogram-force               kgf          9.80665          0.0
kilopond                     kp           9.80665          0.0           The deprecated unit 
                                                                         kilogram-force (kgf) or 
                                                                         kilopond (kp) is defined 
                                                                         as the force exerted by 
                                                                         one kilogram of mass in 
                                                                         standard Earth gravity. 
                                                                         Although the gravitational
                                                                         pull of the Earth varies 
                                                                         as a function of position 
                                                                         on earth, it is here
                                                                         defined as exactly 9.80665
                                                                         m/s^2. So one 
                                                                         kilogram-force is by 
                                                                         definition equal to 
                                                                         9.80665 newtons
kilopound-force              kipf         4448.22161525    0.0
lb·ft/s^2                    lb ft s^-2   0.138254954376   0.0           Same as 1 poundal
newton                       N            1.0              0.0           A newton is the amount of
                                                                         force required to 
                                                                         accelerate a mass of one 
                                                                         kilogram at a rate of one
                                                                         metre per second squared. 
                                                                         In addition, 1N is the 
                                                                         force of gravity on a 
                                                                         small apple on Earth
ounce-force                  ozf          0.278013850953   0.0
pond                         p            0.00980665       0.0
pound-force                  lbf          4.448222         0.0           The pound-force is a 
                                                                         non-SI unit of force or 
                                                                         weight. The pound-force is
                                                                         equal to a mass of one
                                                                         avoirdupois pound (which 
                                                                         is currently defined as 
                                                                         exactly 0.45359237 
                                                                         kilogram) multiplied by 
                                                                         the standard acceleration 
                                                                         due to gravity on Earth. 
                                                                         (The pound-force is thus
                                                                         roughly the force exerted
                                                                         due to gravity by a mass 
                                                                         of one pound at the 
                                                                         surface of the Earth)
poundal                      pdl          0.138254954376   0.0           The poundal is a non-SI 
                                                                         unit of force. It is a 
                                                                         part of the absolute 
                                                                         foot-pound-second system
                                                                         of units, a coherent 
                                                                         subsystem of English units
                                                                         introduced in 1879, and 
                                                                         one of several specialized
                                                                         subsystems of mechanical 
                                                                         units used as aids in 
                                                                         calculations. It is 
                                                                         defined as 1 lb·ft/s^2
tonne-force(metric)          tf           9806.65          0.0
**Speed**
metre/second                 m s^-1       1.0              0.0
foot/hour                    fph          8.4666666666e-05 0.0
inch/minute                  ipm          0.00042333333333 0.0
foot/minute                  fpm          0.00508          0.0
inch/second                  ips          0.0254           0.0
kilometre/hour               km h^-1      0.277777777778   0.0
foot/second                  fps          0.3048           0.0
mile/hour                    mph          0.44704          0.0           
knot(admiralty)              kn           0.514773         0.0           
mile/minute                  mpm          26.8224          0.0           
mile/second                  mps          1609.344         0.0           
speed of light in vacuum     c            299792458.0      0.0           The speed of light in a 
                                                                         vacuum is an important 
                                                                         physical constant denoted
                                                                         by the letter c for
                                                                         constant or the Latin word
                                                                         celeritas meaning
                                                                         'swiftness'. It is the 
                                                                         speed of all 
                                                                         electromagnetic radiation,
                                                                         including visible light,
                                                                         in a vacuum. More 
                                                                         generally, it is the speed
                                                                         of anything having zero 
                                                                         rest mass.
**Monetary value**
US Dollar                    $            1.0              0.0           
**Unit price (volume)**
US Dollar per square metre   $ m^-3       1.0              0.0
**Unit price (mass)**
US Dollar per kilogram       $ kg^-1      1.0              0.0
**Energy price**
USDollars per kilowatt-hour  $ kWh^-1     2.77777777e-07   0.0           Cost in US Dollars per
                                                                         energy unit 
                                                                         (kilowatt-hour).
USDollars per kilojoule      $ kJ^-1      0.001            0.0           Cost in US Dollars per
                                                                         energy unit (kilojoule).
USDollars per joule          $ J^-1       1.0              0.0           Cost in US Dollars per
                                                                         energy unit (joule).
**Specific cost (time)**
US Dollar per second         $ s^-1       1.0              0.0
US Dollar per minute         $ min^-1     0.0166666667     0.0
US Dollar per hour           $ h^-1       0.000277777778   0.0
US Dollar per day            $ day^-1     1.15740741e-05   0.0
US Dollar per month          $ mon^-1     3.80265176e-07   0.0
US Dollar per year           $ yr^-1      3.16887646e-08   0.0
============================ ============ ================ ============= ==========================


Basic concepts
~~~~~~~~~~~~~~

List of unit keywords
~~~~~~~~~~~~~~~~~~~~~
