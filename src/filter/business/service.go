package business

type filterService struct{}

func NewFilterService() FilterService {
	return &filterService{}
}
