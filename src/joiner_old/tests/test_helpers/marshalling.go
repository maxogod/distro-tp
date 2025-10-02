package test_helpers

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/maxogod/distro-tp/src/common/models/data_batch"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"google.golang.org/protobuf/proto"
)

func MarshalPayloadForUsersDataset(t *testing.T, csvPayloads [][]byte) ([]byte, error) {
	t.Helper()

	var users []*raw.User
	for _, csvPayload := range csvPayloads {
		row := string(csvPayload)
		cols := strings.Split(strings.TrimSpace(row), ",")
		if len(cols) < 4 {
			t.Fatalf("invalid csv payload: %s", row)
		}

		userID, _ := strconv.Atoi(cols[0])
		birthdate := cols[2]
		registeredAt := cols[3]

		users = append(users, &raw.User{
			UserId:       int32(userID),
			Gender:       cols[1],
			Birthdate:    birthdate,
			RegisteredAt: registeredAt,
		})
	}

	batch := &raw.UserBatch{Users: users}
	return proto.Marshal(batch)
}

func MarshalPayloadForStoresDataset(t *testing.T, csvPayloads [][]byte) ([]byte, error) {
	t.Helper()

	var stores []*raw.Store
	for _, csvPayload := range csvPayloads {
		fields := strings.Split(strings.TrimSpace(string(csvPayload)), ",")
		if len(fields) < 8 {
			return nil, fmt.Errorf("invalid store csv: %s", string(csvPayload))
		}

		storeID, _ := strconv.Atoi(fields[0])
		postal, _ := strconv.Atoi(fields[3])
		lat, _ := strconv.ParseFloat(fields[6], 64)
		lng, _ := strconv.ParseFloat(fields[7], 64)

		stores = append(stores, &raw.Store{
			StoreId:    int32(storeID),
			StoreName:  fields[1],
			Street:     fields[2],
			PostalCode: int32(postal),
			City:       fields[4],
			State:      fields[5],
			Latitude:   lat,
			Longitude:  lng,
		})
	}

	batch := &raw.StoreBatch{Stores: stores}
	return proto.Marshal(batch)
}

func MarshalPayloadForMenuItemsDataset(t *testing.T, csvPayloads [][]byte) ([]byte, error) {
	t.Helper()

	var items []*raw.MenuItem
	for _, csvPayload := range csvPayloads {
		fields := strings.Split(strings.TrimSpace(string(csvPayload)), ",")
		if len(fields) < 7 {
			return nil, fmt.Errorf("invalid menu item csv: %s", string(csvPayload))
		}

		itemID, err := strconv.Atoi(fields[0])
		if err != nil {
			return nil, err
		}

		price, err := strconv.ParseFloat(fields[3], 64)
		if err != nil {
			return nil, err
		}

		isSeasonal := strings.ToLower(fields[4]) == "true"

		items = append(items, &raw.MenuItem{
			ItemId:        int32(itemID),
			ItemName:      fields[1],
			Category:      fields[2],
			Price:         price,
			IsSeasonal:    isSeasonal,
			AvailableFrom: fields[5],
			AvailableTo:   fields[6],
		})
	}

	batch := &raw.MenuItemBatch{MenuItems: items}
	return proto.Marshal(batch)
}

func GetPayloadForDatasetType(t *testing.T, datasetType enum.RefDatasetType, taskType enum.TaskType, csvPayloads [][]byte) (*data_batch.DataBatch, error) {
	var payload []byte
	var err error

	switch datasetType {
	case enum.Users:
		payload, err = MarshalPayloadForUsersDataset(t, csvPayloads)
	case enum.Stores:
		payload, err = MarshalPayloadForStoresDataset(t, csvPayloads)
	case enum.MenuItems:
		payload, err = MarshalPayloadForMenuItemsDataset(t, csvPayloads)
	default:
		return nil, fmt.Errorf("unknown dataset type: %d", datasetType)
	}

	return &data_batch.DataBatch{
		TaskType: int32(taskType),
		Payload:  payload,
	}, err
}
