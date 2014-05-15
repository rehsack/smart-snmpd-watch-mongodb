#ifndef __ASN1_H_INCLUDED__
#define __ASN1_H_INCLUDED__

#define ASN_BOOLEAN      (0x01)
#ifndef ASN_INTEGER
#define ASN_INTEGER      (0x02)
#endif
#define ASN_BIT_STR      (0x03)
#define ASN_OCTET_STR    (0x04)
#ifndef ASN_NULL
#define ASN_NULL         (0x05)
#endif
#define ASN_OBJECT_ID    (0x06)
#ifndef ASN_SEQUENCE
#define ASN_SEQUENCE     (0x10)
#endif
#define ASN_SET          (0x11)
#ifndef ASN_UNIVERSAL
#define ASN_UNIVERSAL    (0x00)
#endif
#ifndef ASN_APPLICATION
#define ASN_APPLICATION  (0x40)
#endif
#ifndef ASN_CONTEXT
#define ASN_CONTEXT      (0x80)
#endif
#ifndef ASN_PRIVATE
#define ASN_PRIVATE      (0xC0)
#endif
#ifndef ASN_PRIMITIVE
#define ASN_PRIMITIVE    (0x00)
#endif
#ifndef ASN_CONSTRUCTOR
#define ASN_CONSTRUCTOR  (0x20)
#endif
#define ASN_LONG_LEN     (0x80)
#define ASN_EXTENSION_ID (0x1F)
#define ASN_BIT8         (0x80)

#define IS_CONSTRUCTOR(byte)  ((byte) & ASN_CONSTRUCTOR)
#define IS_EXTENSION_ID(byte) (((byte) & ASN_EXTENSION_ID) == ASN_EXTENSION_ID)

#define ASN_UNI_PRIM (ASN_UNIVERSAL | ASN_PRIMITIVE)
#define ASN_SEQ_CON  (ASN_SEQUENCE | ASN_CONSTRUCTOR)

#define SMI_IPADDRESS       (ASN_APPLICATION | 0)
#define SMI_COUNTER         (ASN_APPLICATION | 1)
#define SMI_GAUGE           (ASN_APPLICATION | 2)
#define SMI_TIMETICKS       (ASN_APPLICATION | 3)
#define SMI_OPAQUE          (ASN_APPLICATION | 4)
#define SMI_NSAP            (ASN_APPLICATION | 5)
#define SMI_COUNTER64       (ASN_APPLICATION | 6)
#define SMI_UINTEGER        (ASN_APPLICATION | 7)

#endif /*?__ASN1_H_INCLUDED__*/
