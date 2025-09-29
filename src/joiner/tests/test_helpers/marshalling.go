package test_helpers

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/maxogod/distro-tp/src/common/models"
	"github.com/maxogod/distro-tp/src/common/protocol"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func MarshalPayloadForUsersDataset(t *testing.T, csvPayloads [][]byte) ([]byte, error) {
	t.Helper()

	var users []*protocol.User
	for _, csvPayload := range csvPayloads {
		row := string(csvPayload)
		cols := strings.Split(strings.TrimSpace(row), ",")
		if len(cols) < 4 {
			t.Fatalf("invalid csv payload: %s", row)
		}

		userID, _ := strconv.Atoi(cols[0])
		birthdate := cols[2]
		registeredAt := cols[3]

		users = append(users, &protocol.User{
			UserId:       int32(userID),
			Gender:       cols[1],
			Birthdate:    birthdate,
			RegisteredAt: registeredAt,
		})
	}

	batch := &protocol.Users{Users: users}
	return proto.Marshal(batch)
}

func MarshalPayloadForStoresDataset(t *testing.T, csvPayloads [][]byte) ([]byte, error) {
	t.Helper()

	var stores []*protocol.Store
	for _, csvPayload := range csvPayloads {
		fields := strings.Split(strings.TrimSpace(string(csvPayload)), ",")
		if len(fields) < 8 {
			return nil, fmt.Errorf("invalid store csv: %s", string(csvPayload))
		}

		storeID, _ := strconv.Atoi(fields[0])
		postal, _ := strconv.Atoi(fields[3])
		lat, _ := strconv.ParseFloat(fields[6], 64)
		lng, _ := strconv.ParseFloat(fields[7], 64)

		stores = append(stores, &protocol.Store{
			StoreID:    int32(storeID),
			StoreName:  fields[1],
			Street:     fields[2],
			PostalCode: int32(postal),
			City:       fields[4],
			State:      fields[5],
			Latitude:   lat,
			Longitude:  lng,
		})
	}

	batch := &protocol.Stores{Stores: stores}
	return proto.Marshal(batch)
}

func MarshalPayloadForMenuItemsDataset(t *testing.T, csvPayloads [][]byte) ([]byte, error) {
	t.Helper()

	var items []*protocol.MenuItem
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

		var availableFrom, availableTo *timestamppb.Timestamp
		if len(fields[5]) > 0 {
			timeAvailableFrom, timeErr := time.Parse("2006-01-02", fields[5])
			if timeErr != nil {
				return nil, fmt.Errorf("invalid available_from date: %s", fields[5])
			}
			availableFrom = timestamppb.New(timeAvailableFrom)
		}
		if len(fields[6]) > 0 {
			timeAvailableTo, timeErr := time.Parse("2006-01-02", fields[6])
			if timeErr != nil {
				return nil, fmt.Errorf("invalid available_to date: %s", fields[6])
			}
			availableTo = timestamppb.New(timeAvailableTo)
		}

		items = append(items, &protocol.MenuItem{
			ItemId:        int32(itemID),
			ItemName:      fields[1],
			Category:      fields[2],
			Price:         price,
			IsSeasonal:    isSeasonal,
			AvailableFrom: availableFrom,
			AvailableTo:   availableTo,
		})
	}

	batch := &protocol.MenuItems{Items: items}
	return proto.Marshal(batch)
}

func GetPayloadForDatasetType(t *testing.T, datasetType models.RefDatasetType, csvPayloads [][]byte) (*protocol.ReferenceBatch, error) {
	var payload []byte
	var err error

	switch datasetType {
	case models.Users:
		payload, err = MarshalPayloadForUsersDataset(t, csvPayloads)
	case models.Stores:
		payload, err = MarshalPayloadForStoresDataset(t, csvPayloads)
	case models.MenuItems:
		payload, err = MarshalPayloadForMenuItemsDataset(t, csvPayloads)
	default:
		return nil, fmt.Errorf("unknown dataset type: %d", datasetType)
	}

	return &protocol.ReferenceBatch{
		DatasetType: int32(datasetType),
		Payload:     payload,
	}, err
}
