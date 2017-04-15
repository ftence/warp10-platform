namespace java io.warp10.quasar.token.thrift.data

enum TokenType {
    READ = 1,
    WRITE = 2,
}

/**
 * The write token contains all the data necessary for remove all data lookup calls.
 * He can be used only for put data on Ingress
 */
struct WriteToken {
    /**
     * delivery timestamp of the token
     */
    1:required i64 issuanceTimestamp,

    /**
     * Timestamp of the token expiration
     */
    2:required i64 expiryTimestamp,

    /**
     * TokenType (Set to TokenType.WRITE)
     */
    3:required TokenType tokenType

    /**
     * true if a lookup is necessary, default false
     */
    4:optional bool lookup,

    /**
     * ident of the producer
     * a producer inject data, but is not the data owner
     */
    5:required binary producerId,

    /**
     * ident of the owner
     */
    6:required binary ownerId,

    /**
     * Application name
     */
    7:required string appName

    /**
     * indices to use
     */
    8:optional list<i64> indices,

    /**
     * default labels
     */
    9:optional map<string,string> labels,
    
    /**
     * Token attributes, placeholder to store K/V
     */
    10: optional map<string,string> attributes,
}

/**
 * The read token contains all the data necessary for remove all data lookup calls.
 * He can be used only for data access
 */
struct ReadToken {
    /**
     * delivery timestamp of the token
     */
    1:required i64 issuanceTimestamp,

    /**
     * Timestamp of the token expiration
     */
    2:required i64 expiryTimestamp,

    /**
     * TokenType (Set to TokenType.READ)
     */
    3:required TokenType tokenType

    /**
     * true if a lookup is necessary, default false
     */
    4:optional bool lookup,

    /**
     * Groovy scripting enabled, default false
     */
    5:optional bool groovy,

    /**
     * Max fetch size in bytes of each read request, default 16Mb
     */
    6:optional i64 maxFetchSize,

    /**
     * Name of the application using the token
     */
    7:optional string appName,

    /**
     * Token Rights matrix
     * Apps owners and producers appearing in the 3 lists bellow will be forced as labels selectors
     *
     */

    /**
     * List of applications consumed
     */
    8:required list<string> apps,

    /**
     * List of owners
     */
    9:required list<binary> owners,

    /**
     * List of producers
     */
    10:required list<binary> producers,

    /**
     * Id of the billed customer
     */
    11:required binary billedId,

    /**
     * Token hooks
     */
    12:optional map<string,string> hooks,
    
    /**
     * Token attributes
     */
    13:optional map<string,string> attributes,

    /**
     * Token labels
     */
    14:optional map<string,string> labels,

    /**
     * The read token is used for data visualisation, default false
     */
    100:optional bool dataviz,
}
