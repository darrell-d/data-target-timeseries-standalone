package pennsieve

import (
	"math"
	"strings"
)

// ChannelUnit is fixed at microvolts. Upstream processing converts other
// units to uV before this code runs, so the wire payload always sets
// unit="uV". Mirrors processor/timeseries_channel.py UNIT.
const ChannelUnit = "uV"

// TimeSeriesChannel is the metadata for one channel in a time-series
// recording, as exchanged with the legacy Pennsieve timeseries API.
//
// Mirrors processor/timeseries_channel.py from processor-post-timeseries.
// Some fields ride along on each instance for convenience even though
// they aren't on every wire payload (Index, Properties).
type TimeSeriesChannel struct {
	// Index is the intra-processor channel ordinal (e.g. "channel-00007"),
	// used by orchestration to map staged chunk filenames to channel
	// records. Not part of any API payload.
	Index string

	// ID is the Pennsieve node id (e.g. "N:channel:..."). Empty before
	// the channel has been created server-side.
	ID string

	// ViewerAssetID is the FK to the viewer_asset row that owns this
	// channel. Required at create time for the asset-flow ingest.
	ViewerAssetID string

	Name           string
	Rate           float64
	Start          int64
	End            int64
	Type           string // CONTINUOUS or UNIT
	Group          string
	LastAnnotation int64
	Properties     []map[string]any
}

// CreateRequestBody returns the JSON-serializable map for
// POST /timeseries/{pkg}/channels. The legacy API expects "channelType"
// (not "type") on the wire; this method handles that translation.
func (c *TimeSeriesChannel) CreateRequestBody() map[string]any {
	properties := c.Properties
	if properties == nil {
		properties = []map[string]any{}
	}
	body := map[string]any{
		"name":           strings.TrimSpace(c.Name),
		"start":          c.Start,
		"end":            c.End,
		"unit":           ChannelUnit,
		"rate":           c.Rate,
		"channelType":    strings.ToUpper(c.Type),
		"group":          strings.TrimSpace(c.Group),
		"lastAnnotation": c.LastAnnotation,
		"properties":     properties,
	}
	if c.ID != "" {
		body["id"] = c.ID
	}
	if c.ViewerAssetID != "" {
		body["viewerAssetId"] = c.ViewerAssetID
	}
	return body
}

// EqualForReuse reports whether `other` is the same logical channel as
// `c` for the purpose of reusing an existing record on the package.
// Mirrors TimeSeriesChannel.__eq__: case-insensitive name + type match
// and rate within a 2% tolerance.
func (c *TimeSeriesChannel) EqualForReuse(other *TimeSeriesChannel) bool {
	if other == nil {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(c.Name), strings.TrimSpace(other.Name)) {
		return false
	}
	if !strings.EqualFold(c.Type, other.Type) {
		return false
	}
	if other.Rate == 0 {
		return false
	}
	return math.Abs(1-(c.Rate/other.Rate)) < 0.02
}

// channelResponse is the wire shape returned by the legacy timeseries
// API: each entry wraps the channel `content` and a separate `properties`
// list. Used by both single-channel responses (POST /channels) and list
// responses (GET /channels).
type channelResponse struct {
	Content struct {
		ID             string  `json:"id,omitempty"`
		Name           string  `json:"name"`
		Rate           float64 `json:"rate"`
		Start          int64   `json:"start"`
		End            int64   `json:"end"`
		Type           string  `json:"type,omitempty"`
		ChannelType    string  `json:"channelType,omitempty"`
		Group          string  `json:"group"`
		LastAnnotation int64   `json:"lastAnnotation"`
		ViewerAssetID  string  `json:"viewerAssetId,omitempty"`
	} `json:"content"`
	Properties []map[string]any `json:"properties"`
}

// toChannel converts the wire response shape into our domain struct.
// The legacy API may return the channel type under either "type" or
// "channelType" depending on the endpoint; prefer the latter.
func (r *channelResponse) toChannel() *TimeSeriesChannel {
	typ := r.Content.ChannelType
	if typ == "" {
		typ = r.Content.Type
	}
	return &TimeSeriesChannel{
		ID:             r.Content.ID,
		ViewerAssetID:  r.Content.ViewerAssetID,
		Name:           r.Content.Name,
		Rate:           r.Content.Rate,
		Start:          r.Content.Start,
		End:            r.Content.End,
		Type:           typ,
		Group:          r.Content.Group,
		LastAnnotation: r.Content.LastAnnotation,
		Properties:     r.Properties,
	}
}
