package amqp

// SASL Codes
const (
	codeSASLOK      saslCode = iota // Connection authentication succeeded.
	codeSASLAuth                    // Connection authentication failed due to an unspecified problem with the supplied credentials.
	codeSASLSys                     // Connection authentication failed due to a system error.
	codeSASLSysPerm                 // Connection authentication failed due to a system error that is unlikely to be corrected without intervention.
	codeSASLSysTemp                 // Connection authentication failed due to a transient system error.
)

type saslCode int

func (s *saslCode) unmarshal(r byteReader) error {
	_, err := unmarshal(r, (*int)(s))
	return err
}

// SASL Mechanisms
const (
	saslMechanismPLAIN Symbol = "PLAIN"
)

// ConnSASLPlain enables SASL PLAIN authentication for the connection.
func ConnSASLPlain(username, password string) ConnOption {
	return func(c *Conn) error {
		if c.saslHandlers == nil {
			c.saslHandlers = make(map[Symbol]stateFunc)
		}
		c.saslHandlers[saslMechanismPLAIN] = (&saslHandlerPlain{
			c:        c,
			username: username,
			password: password,
		}).init
		return nil
	}
}

type saslHandlerPlain struct {
	c        *Conn
	username string
	password string
}

func (h *saslHandlerPlain) init() stateFunc {
	h.c.txFrame(frame{
		typ: frameTypeSASL,
		body: &saslInit{
			Mechanism:       "PLAIN",
			InitialResponse: []byte("\x00" + h.username + "\x00" + h.password),
			Hostname:        "",
		},
	})

	return h.c.saslOutcome
}

/*
<type name="sasl-init" class="composite" source="list" provides="sasl-frame">
    <descriptor name="amqp:sasl-init:list" code="0x00000000:0x00000041"/>
    <field name="mechanism" type="symbol" mandatory="true"/>
    <field name="initial-response" type="binary"/>
    <field name="hostname" type="string"/>
</type>
*/

type saslInit struct {
	Mechanism       Symbol
	InitialResponse []byte
	Hostname        string
}

func (si *saslInit) link() (uint32, bool) {
	return 0, false
}

func (si *saslInit) marshal() ([]byte, error) {
	return marshalComposite(typeCodeSASLInit, []marshalField{
		{value: si.Mechanism, omit: false},
		{value: si.InitialResponse, omit: len(si.InitialResponse) == 0},
		{value: si.Hostname, omit: len(si.Hostname) == 0},
	}...)
}

/*
<type name="sasl-mechanisms" class="composite" source="list" provides="sasl-frame">
    <descriptor name="amqp:sasl-mechanisms:list" code="0x00000000:0x00000040"/>
    <field name="sasl-server-mechanisms" type="symbol" multiple="true" mandatory="true"/>
</type>
*/

type saslMechanisms struct {
	Mechanisms []Symbol
}

func (sm *saslMechanisms) unmarshal(r byteReader) error {
	return unmarshalComposite(r, typeCodeSASLMechanism,
		unmarshalField{field: &sm.Mechanisms, handleNull: required("SASLMechanisms.Mechanisms")},
	)
}

func (*saslMechanisms) link() (uint32, bool) {
	return 0, false
}

/*
<type name="sasl-outcome" class="composite" source="list" provides="sasl-frame">
    <descriptor name="amqp:sasl-outcome:list" code="0x00000000:0x00000044"/>
    <field name="code" type="sasl-code" mandatory="true"/>
    <field name="additional-data" type="binary"/>
</type>
*/

type saslOutcome struct {
	Code           saslCode
	AdditionalData []byte
}

func (so *saslOutcome) unmarshal(r byteReader) error {
	return unmarshalComposite(r, typeCodeSASLOutcome, []unmarshalField{
		{field: &so.Code, handleNull: required("SASLOutcome.Code")},
		{field: &so.AdditionalData},
	}...)
}

func (*saslOutcome) link() (uint32, bool) {
	return 0, false
}
