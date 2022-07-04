package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"zebrax.id/emi/integration/core/utils"
	"zebrax.id/emi/integration/erp/connector/odoo/model"
	"zebrax.id/emi/integration/erp/connector/odoo/repository/query"
)

func jMarshal(response interface{}) (result model.PreOrderResponse) {
	jsonStr, err := json.Marshal(response)
	if err != nil {
		log.Error(err)
	}

	list := new(model.PreOrderResponse)
	if err := json.Unmarshal(jsonStr, &result); err != nil {
		log.Error(err)
	}

	return *list
}

func (r *repository) SetBookingTestDrive(bookParams model.BookParams) (list model.BookingTestDriveResponse, err error) {
	defer log.Info("[Odoo - Connector - SetBookingTestDrive] End")
	log.Info("[Odoo - Connector - SetBookingTestDrive] Start Type : ", bookParams.BookingTypeID)
	// Set Booking Test Drive into DB
	// Success Output Sample : "0|Inserting Succesfully TD/D0202/22/00187|733|1|Product 1|TD/D0202/22/00187|2022-03-01|2022-03-01T11:00:00+07:00|2022-03-01T12:00:00+07:00|1|Indy Office Bintaro|Jl. Al Hidayah No.44, Pd. Jaya, Kec. Pd. Aren |-6.27466|106.72046|Kota Tangerang Selatan|Banten|Indonesia|Everydays 10.00 - 18.00"
	// Error Output Sample : "1| Slot ID not exists in database|0|||||||||||||||"
	// the output should be split and get each item for the return value
	result := ""
	if bookParams.BookingTypeID == 1 {
		log.Info("[Odoo - Connector - SetBookingTestDrive] Function SetBookingTestDriveV2")
		result, err = r.qry.SetBookingTestDriveV2(context.Background(), &query.SetBookingTestDriveV2Params{
			FnBookingTestdriveV2:   "I",
			FnBookingTestdriveV2_2: bookParams.EcID,
			FnBookingTestdriveV2_3: bookParams.ProductID,
			FnBookingTestdriveV2_4: bookParams.BookingTypeID,
			FnBookingTestdriveV2_5: bookParams.SlotDate,
			FnBookingTestdriveV2_6: bookParams.SlotStartTime,
			FnBookingTestdriveV2_7: bookParams.UID,
		})
	} else {
		log.Info("[Odoo - Connector - SetBookingTestDrive] Function SetBookingTestDriveOnWheel")
		result, err = r.qry.SetBookingTestDriveOnWheel(context.Background(), &query.SetBookingTestDriveOnWheelParams{
			FnBookingTestdriveOnwheelsV2:    "I",
			FnBookingTestdriveOnwheelsV2_2:  bookParams.EcID,
			FnBookingTestdriveOnwheelsV2_3:  bookParams.ProductID,
			FnBookingTestdriveOnwheelsV2_4:  bookParams.BookingTypeID,
			FnBookingTestdriveOnwheelsV2_5:  bookParams.SlotDate,
			FnBookingTestdriveOnwheelsV2_6:  bookParams.SlotStartTime,
			FnBookingTestdriveOnwheelsV2_7:  bookParams.UID,
			FnBookingTestdriveOnwheelsV2_8:  bookParams.Address,
			FnBookingTestdriveOnwheelsV2_9:  "",
			FnBookingTestdriveOnwheelsV2_10: bookParams.City,
			FnBookingTestdriveOnwheelsV2_11: bookParams.Notes,
			FnBookingTestdriveOnwheelsV2_12: bookParams.Latitude,
			FnBookingTestdriveOnwheelsV2_13: bookParams.Longitude,
		})
	}
	if err != nil {
		log.Info("[Odoo - Connector - SetBookingTestDrive] Error ", err.Error())
		return list, err
	}
	bookResult := strings.Split(result, utils.ConnectorOdooSeparator)

	list.Code = bookResult[0]
	if bookResult[0] == "0" {
		log.Info("[Odoo - Connector - SetBookingTestDrive] RPC em.appointment.system -  action_confirm: ", bookResult[2])
		bookingId, _ := utils.StringToInt(bookResult[2])
		_, err := r.rpc.ExecuteKw("action_confirm", "em.appointment.system", []interface{}{
			[]interface{}{bookingId},
		}, nil)

		if err != nil {
			list.Code = "1"
			list.Message = err.Error()
			log.Info("[Odoo - Connector - SetBookingTestDrive] RPC em.appointment.system -  action_confirm Error: ", err.Error())
			return list, err
		}

		list.Message = bookResult[1] + " " + bookResult[5]
		list.ProductID = bookResult[3]
		list.ProductName = bookResult[4]
		list.BookingID = bookResult[2]
		list.BookingCode = bookResult[5]
		list.Date = bookResult[6]
		list.StartTime = bookResult[7]
		list.EndTime = bookResult[8]
		list.LocationID = bookResult[9]
		list.LocationName = bookResult[10]
		list.Address = bookResult[11]
		list.Longitude = bookResult[13]
		list.Latitude = bookResult[12]
		list.City = bookResult[14]
		list.State = bookResult[15]
		list.Country = bookResult[16]
		list.OperatingHours = bookResult[17]
		list.Notes = ""
	} else {
		list.Message = bookResult[1]
		log.Info("[Odoo - Connector - SetBookingTestDrive] Error ", bookResult[1])
	}

	return list, nil
}

func (r *repository) SetRescheduleBookingTestDrive(bookParams model.BookParams) (list model.BookingTestDriveResponse, err error) {
	// Set Reschedule Booking Test Drive into DB
	// Success Output Sample : "0|Inserting Succesfully TD/D0202/22/00187|733|1|Product 1|TD/D0202/22/00187|2022-03-01|2022-03-01T11:00:00+07:00|2022-03-01T12:00:00+07:00|1|Indy Office Bintaro|Jl. Al Hidayah No.44, Pd. Jaya, Kec. Pd. Aren |-6.27466|106.72046|Kota Tangerang Selatan|Banten|Indonesia|Everydays 10.00 - 18.00"
	// Error Output Sample : "1| Slot ID not exists in database|0|||||||||||||||"
	// the output should be split and get each item for the return value
	result, err := r.qry.SetReschedulerBookingTestDrive(context.Background(), &query.SetReschedulerBookingTestDriveParams{
		FnBookingTestdriveRescheduleV2:   bookParams.BookingID,
		FnBookingTestdriveRescheduleV2_2: bookParams.EcID,
		FnBookingTestdriveRescheduleV2_3: bookParams.ProductID,
		FnBookingTestdriveRescheduleV2_4: bookParams.BookingTypeID,
		FnBookingTestdriveRescheduleV2_5: bookParams.SlotDate,
		FnBookingTestdriveRescheduleV2_6: bookParams.SlotStartTime,
		FnBookingTestdriveRescheduleV2_7: bookParams.UID,
	})

	if err != nil {
		list.Code = "1"
		list.Message = err.Error()
		return list, err
	}

	bookResult := strings.Split(result, utils.ConnectorOdooSeparator)

	list.Code = bookResult[0]
	if bookResult[0] == "0" {
		list.Message = bookResult[1]
		list.ProductID = bookResult[3]
		list.ProductName = bookResult[4]
		list.BookingID = bookResult[2]
		list.BookingCode = bookResult[5]
		list.Date = bookResult[6]
		list.StartTime = bookResult[7]
		list.EndTime = bookResult[8]
		list.LocationID = bookResult[9]
		list.LocationName = bookResult[10]
		list.Address = bookResult[11]
		list.Longitude = bookResult[13]
		list.Latitude = bookResult[12]
		list.City = bookResult[14]
		list.State = bookResult[15]
		list.Country = bookResult[16]
		list.OperatingHours = bookResult[17]
	} else {
		list.Message = bookResult[1]
	}

	return list, nil
}

func (r *repository) SetCancelBookingTestDrive(bookParams model.CancelBookingTestDriveParams) (list model.BookingTestDriveResponse, err error) {
	// Set Cancel Booking Test Drive into DB
	// the output should be split and get each item for the return value
	result, err := r.qry.SetCancelBookingTestDrive(context.Background(), &query.SetCancelBookingTestDriveParams{
		SpBookingTestdriveCancel:   bookParams.BookingID,
		SpBookingTestdriveCancel_2: bookParams.CategoryID,
		SpBookingTestdriveCancel_3: bookParams.Comment,
		SpBookingTestdriveCancel_4: bookParams.UpdateBy,
	})

	list.ID = fmt.Sprintf("%d", bookParams.BookingID)

	if err != nil {
		list.Code = "1"
		list.Message = err.Error()
		return list, err
	}

	cancelResult := strings.Split(result, utils.ConnectorOdooSeparator)

	list.Code = cancelResult[0]
	list.Message = cancelResult[1]

	return list, nil
}

func (r *repository) SetVoucherRedeem(salesOrderId int32, voucherId int32) (list model.OrderConfirmationResponses, err error) {
	if salesOrderId == 0 || voucherId == 0 {
		return list, err
	}

	// Check Availability of vouchers based on SoId
	// Output Sample : "0|Searching Get Succesfully 6968773680224492744|6968773680224492744|Y"
	// the output should be split, take the second and third index
	resultGetVoucherCode, _ := r.qry.GetVoucherCodeBySoIdAndVoucherId(context.Background(), &query.GetVoucherCodeBySoIdAndVoucherIdParams{
		FnGetVoucherCode:   salesOrderId,
		FnGetVoucherCode_2: voucherId,
	})
	vouchers := strings.Split(resultGetVoucherCode, utils.ConnectorOdooSeparator)

	if vouchers[3] == "N" {
		log.Info(fmt.Sprintf("[Odoo - Connector - SetVoucherRedeemSetVoucherRedeem] sale.coupon.apply.code - process_coupon_so Params SalesOrderId: %d, couponCode: %s\n\n", salesOrderId, vouchers[2]))
		_, err = r.rpc.ExecuteKw("process_coupon_so", "sale.coupon.apply.code", []interface{}{
			map[string]interface{}{
				"order_id":    salesOrderId,
				"coupon_code": vouchers[2],
			},
		}, nil)
		if err != nil {
			fmt.Printf("[Odoo - Connector - SetVoucherRedeemSetVoucherRedeem] Error : \n%s\n", err.Error())
			return list, err
		}

		// Output Sample : "0|Searching Get Succesfully |10900|15"
		// the output should be split, take the second index for computing the amount
		getVoucherLine, _ := r.qry.GetVoucherLineBySoId(context.Background(), salesOrderId)
		voucherLine := strings.Split(getVoucherLine, utils.ConnectorOdooSeparator)
		voucherLineId, _ := utils.StringToInt(voucherLine[2])
		log.Info(fmt.Sprintf("[Odoo - Connector - SetVoucherRedeemSetVoucherRedeem] sale.order.line - compute_amount Params couponCode: %s\n\n", voucherLine[2]))
		_, err = r.rpc.ExecuteKw("compute_amount", "sale.order.line", []interface{}{
			[]interface{}{voucherLineId},
		}, nil)
		if err != nil {
			fmt.Printf("[Odoo - Connector - SetVoucherRedeemSetVoucherRedeem] sale.order.line - compute_amount : \n%s\n", err.Error())
			return list, err
		}
	}

	soDetail, _ := r.qry.GetSoDetailBySoId(context.Background(), salesOrderId)
	if soDetail != "" {
		json.Unmarshal([]byte(soDetail), &list)
	}

	return list, nil
}

func (r *repository) GetEvAvailable() (list []model.EvAvailable, err error) {
	var (
		mapping = make(map[int32]*model.EvAvailable)
	)

	getList, err := r.qry.GetAllEvAvailable(context.Background())
	for _, row := range getList {
		if _, ok := mapping[row.LocationID]; !ok {
			mapping[row.LocationID] = &model.EvAvailable{
				LocationID:  row.LocationID,
				Street:      row.Street,
				Street2:     row.Street2.String,
				CityID:      row.CityID,
				City:        row.City,
				State:       row.State,
				Longitude:   row.Longitude,
				Latitude:    row.Latitude,
				CompanyName: row.CompanyName,
			}
		}

		mapping[row.LocationID].Products = append(mapping[row.LocationID].Products, model.Product{
			ProductID:   row.ProductID,
			ProductCode: row.ProductCode,
			ProductName: row.ProductName,
		})
	}

	for _, row := range mapping {
		list = append(list, *row)
	}

	return list, err
}

func (r *repository) GetProductTemplatePrice(dealerId int32, productCode string) (list []model.ProductTemplate, err error) {
	productTemplate, err := r.qry.GetProductTemplate(context.Background(), &query.GetProductTemplateParams{
		FnApiProducttemplatePricelist:   dealerId,
		FnApiProducttemplatePricelist_2: productCode,
	})
	if productTemplate != "" {
		json.Unmarshal([]byte(productTemplate), &list)
	}

	return
}

func (r *repository) GetVoucherList(salesOrderId int32) (list model.Voucher, err error) {
	voucherList, err := r.qry.GetVoucherList(context.Background(), salesOrderId)
	if voucherList != "" {
		json.Unmarshal([]byte(voucherList), &list)
	}

	return
}

func (r *repository) SetOrderConfirmation(purchaseParams model.PurchaseParams) (result model.OrderConfirmationResponses, err error) {
	defer log.Info("[Odoo - Connector - SetOrderConfirmation] End")
	log.Info("[Odoo - Connector - SetOrderConfirmation] Start")

	var (
		colorVariantId, batteryVariantId, wheelVariantId, mirrorVarianId string = "0", "0", "0", "0"
		vehicleCode                                                      string = ""
	)

	uId, _ := utils.StringToInt(purchaseParams.CustomerID)
	dealerId, _ := utils.StringToInt(purchaseParams.DealerID)
	orderId, _ := utils.StringToInt(purchaseParams.SalesOrderID)

	for _, order := range purchaseParams.Orders {
		vehicleCode = order.ProductCode
		for _, attr := range order.Attributes {
			switch attr.AttributeID {
			case "4":
				mirrorVarianId = attr.VariantID
			case "5":
				wheelVariantId = attr.VariantID
			case "10":
				colorVariantId = attr.VariantID
			case "11":
				batteryVariantId = attr.VariantID
			}
		}
	}

	if purchaseParams.CustomerID == "0" || purchaseParams.CustomerID == "" {
		log.Info("[Odoo - Connector - SetOrderConfirmation] For Guest")
		getProductResult, err := r.qry.GetProductIdAsGuest(context.Background(), &query.GetProductIdAsGuestParams{
			FnGetProductIDGuest:   dealerId,
			FnGetProductIDGuest_2: uId,
			FnGetProductIDGuest_3: vehicleCode,
			FnGetProductIDGuest_4: colorVariantId,
			FnGetProductIDGuest_5: batteryVariantId,
			FnGetProductIDGuest_6: mirrorVarianId,
			FnGetProductIDGuest_7: wheelVariantId,
		})
		if err != nil {
			return result, err
		}

		// Sample Output : 0|Searching Product Succesfully A11113|104|33000000|1|EV-V Sporty Single Battery|4|A11113|29|Grey|1|Color|30.000.000||0|33000000||0|3000000|30000000|30000000
		productResult := strings.Split(getProductResult, utils.ConnectorOdooSeparator)

		result.Code = productResult[0]
		result.Message = productResult[1]

		// If Code == "1" then return
		if productResult[0] == "1" {
			log.Info("[Odoo - Connector - SetOrderConfirmation] Error ", productResult[1])
			return result, errors.New(productResult[1])
		}

		grandTotal, _ := utils.StringToInt(productResult[15])

		attributes := []model.Attribute{}
		attributes = append(attributes, model.Attribute{
			AttributeID:   productResult[10],
			AttributeName: productResult[11],
			VariantID:     productResult[8],
			VariantName:   productResult[9],
			Label:         "Included",
			ProductCode:   "",
			Stock:         "",
		})

		result.SoID = "0"
		result.SoNumber = ""
		result.AmountUntaxed = productResult[19]
		result.Tax = productResult[18]
		result.Total = int32(grandTotal)
		result.GrandTotal = productResult[15]
		result.Purchase.Total = productResult[20]
		result.Purchase.Items = append(result.Purchase.Items, model.OrderConfirmationAttributes{
			OdooName:   productResult[5],
			OdooValue:  productResult[3],
			Label:      productResult[12],
			Attributes: attributes,
		})

		result.Administrations.Total = productResult[18]
		result.Tax = productResult[18]

		//If voucher applied
		if productResult[14] != "0" {
			reductionValue, _ := utils.StringToInt(productResult[14])
			result.Reductions.Items = append(result.Reductions.Items, model.OrderConfirmationAttributes{
				Name:          productResult[13],
				Value:         (int64(reductionValue) * -1),
				Label:         productResult[17],
				ReductionType: "discount",
			})
			result.Reductions.Total = fmt.Sprintf("-%d", (int64(reductionValue) * -1))
		}

		return result, err
	}

	if orderId == 0 {
		paramsGetProductId := &query.GetProductIdParams{
			FnGetProductIDV2:   dealerId,
			FnGetProductIDV2_2: uId,
			FnGetProductIDV2_3: vehicleCode,
			FnGetProductIDV2_4: colorVariantId,
			FnGetProductIDV2_5: batteryVariantId,
			FnGetProductIDV2_6: mirrorVarianId,
			FnGetProductIDV2_7: wheelVariantId,
		}
		log.Info(fmt.Sprintf("[Odoo - Connector - SetOrderConfirmation] GetProductId Params : \n%#v\n", paramsGetProductId))
		getProductSoResult, err := r.qry.GetProductId(context.Background(), paramsGetProductId)
		if err != nil {
			log.Info(fmt.Sprintf("[Odoo - Connector - SetOrderConfirmation] GetProductId Query Error: \n%s\n", err.Error()))
			return result, err
		}

		// Sample Output : 0|Searching Product Succesfully A11113|104|33000000|1|EV-V Sporty Single Battery|4
		log.Info(fmt.Sprintf("[Odoo - Connector - SetOrderConfirmation] GetProductId ouput string: \n%s\n", getProductSoResult))
		productSoResult := strings.Split(getProductSoResult, utils.ConnectorOdooSeparator)

		result.Code = productSoResult[0]
		result.Message = productSoResult[1]

		// If Code == "1" then return
		if productSoResult[0] == "1" {
			log.Info(fmt.Sprintf("[Odoo - Connector - SetOrderConfirmation] GetProductId Error : \n%s\n", productSoResult[1]))
			return result, err
		}

		priceListId, _ := utils.StringToInt(productSoResult[6])
		productId, _ := utils.StringToInt(productSoResult[2])
		unitPrice, _ := utils.StringToFloat64(productSoResult[3])
		uomId, _ := utils.StringToInt(productSoResult[4])
		prodName := productSoResult[5]

		log.Info("[Odoo - Connector - SetOrderConfirmation] Execute Sale.Order - Create")
		params := map[string]interface{}{
			"partner_id":            uId,
			"sale_order_type":       2,
			"company_id":            dealerId,
			"pricelist_id":          priceListId,
			"show_update_pricelist": true,
			"state":                 "draft",
		}
		log.Info(fmt.Sprintf("[Odoo - Connector - SetOrderConfirmation] Execute Sale.Order with Params: \n%#v\n", params))
		getOrderId, err := r.rpc.ExecuteKw("create", "sale.order", []interface{}{
			[]interface{}{
				params,
			},
		}, nil)

		if err != nil {
			return result, err
		}

		log.Info("[Odoo - Connector - SetOrderConfirmation] Execute Sale.Order.Line - Create Order Id Original : ", getOrderId)
		orderId, _ = utils.StringToInt(removeFirstAndLastChar(fmt.Sprintf("%d", getOrderId)))
		params = map[string]interface{}{
			"order_id":        orderId,
			"product_id":      productId,
			"name":            prodName,
			"product_uom":     uomId,
			"product_uom_qty": 1,
			"price_unit":      unitPrice,
			"price_total":     unitPrice,
		}
		log.Info(fmt.Sprintf("[Odoo - Connector - SetOrderConfirmation] Execute Sale.Order.Line with Params: \n%#v\n", params))
		_, err = r.rpc.ExecuteKw("create", "sale.order.line", []interface{}{
			[]interface{}{
				params,
			},
		}, nil)

		if err != nil {
			return result, err
		}

		log.Info("[Odoo - Connector - SetOrderConfirmation] Execute Sale.Order - recompute_coupon_lines params order Id: ", orderId)
		_, err = r.rpc.ExecuteKw("recompute_coupon_lines", "sale.order", []interface{}{
			[]interface{}{
				orderId,
			},
		}, nil)

		if err != nil {
			return result, err
		}
	}

	log.Info("[Odoo - Connector - SetOrderConfirmation] Get So Detail By SoId : ", orderId)
	soDetail, _ := r.qry.GetSoDetailBySoId(context.Background(), orderId)
	if soDetail != "" {
		json.Unmarshal([]byte(soDetail), &result)
	}

	return result, err
}

func removeFirstAndLastChar(a string) string {
	removeFirst := a[1:]
	return removeFirst[:len(removeFirst)-1]
}

func (r *repository) GetTestDriveListByUid(uId string) (list []model.BookingTestDriveResponse, err error) {
	defer log.Info("[Odoo - Connector - GetTestDriveListByUid] End")
	log.Info("[Odoo - Connector - GetTestDriveListByUid] Start")

	log.Info(fmt.Sprintf("[Odoo - Connector - GetTestDriveListByUid] Get Data Test Drive : User: %s", uId))
	userId, _ := utils.StringToInt32(uId)
	listTestDrives, err := r.qry.GetTestDriveListByCustomerView(context.Background(), sql.NullInt32{Int32: userId, Valid: true})
	for _, row := range listTestDrives {
		list = append(list, model.BookingTestDriveResponse{
			ProductID:          fmt.Sprintf("%d", row.ProductID.Int32),
			ProductName:        row.ProductName.String,
			BookingID:          fmt.Sprintf("%d", row.BookingID.Int32),
			BookingCode:        row.BookingCode.String,
			Date:               row.Date.String,
			StartTime:          utils.InterfaceToString(row.StartTime.String),
			EndTime:            utils.InterfaceToString(row.EndTime.String),
			LocationID:         fmt.Sprintf("%d", row.EcID.Int32),
			LocationName:       row.EcName.String,
			Address:            utils.InterfaceToString(row.EcAddress.String),
			Address2:           "",
			City:               row.CityName.String,
			State:              row.StateName.String,
			Country:            row.CountryName.String,
			Longitude:          row.EcLongitude.String,
			Latitude:           row.EcLatitude.String,
			OperatingHours:     row.OperationalHours.String,
			BookingStatus:      row.BookingStatus.String,
			CancelCategoryID:   int(row.CancelCategoryID.Int32),
			CancelCategoryText: row.CancelCategoryText.String,
			CancelDate:         row.CancelDate.String,
			Comment:            row.CancelComment.String,
			AppointmentTypeID:  row.AppointmentTypeID.String,
		})
	}
	if err != nil {
		return nil, err
	}

	return list, nil
}

func (r *repository) GetTestDriveTimeSlot(productId string, EcId int32, startDate string, endDate string, appointmentTypeId int32) (list []model.SlotTimeResponses, err error) {
	defer log.Info("[Odoo - Connector - GetTestDriveTimeSlot] End")
	log.Info("[Odoo - Connector - GetTestDriveTimeSlot] Start")

	log.Info("[Odoo - Connector - GetTestDriveTimeSlot] Exec Disable the previous day's slotTime")
	err = r.qry.SetSlotTimeDisable(context.Background())
	if err != nil {
		log.Info("[Odoo - Connector - GetTestDriveTimeSlot] Exec Disable the previous day's slotTime Error: ", err.Error())
		return nil, err
	}

	layout := "2006-01-02"
	startDateFormat, _ := time.Parse(layout, startDate)
	endDateFormat, _ := time.Parse(layout, endDate)
	pId, _ := utils.StringToInt32(productId)
	if appointmentTypeId == 2 {
		log.Info(fmt.Sprintf("[Odoo - Connector - GetTestDriveTimeSlot Onwheels] Get Data Slot with Params ProductId : %s, EcId: %d, startDate: %s, endDate: %s, AppointmentTypeId: %d", productId, EcId, startDate, endDate, appointmentTypeId))
		return r.timeSlotOnWheels(pId, EcId, startDateFormat, endDateFormat, appointmentTypeId)
	}

	log.Info(fmt.Sprintf("[Odoo - Connector - GetTestDriveTimeSlot Standard] Get Data Slot with Params ProductId : %s, EcId: %d, startDate: %s, endDate: %s, AppointmentTypeId: %d", productId, EcId, startDate, endDate, appointmentTypeId))
	return r.timeSlot(pId, EcId, startDateFormat, endDateFormat, appointmentTypeId)
}

func (r *repository) timeSlot(productId int32, EcId int32, startDateFormat time.Time, endDateFormat time.Time, appointmentTypeId int32) (list []model.SlotTimeResponses, err error) {
	var (
		mapping = make(map[string]*model.SlotTimeResponses)
	)

	slotTimeRow, err := r.qry.GetSlotTime(context.Background(), &query.GetSlotTimeParams{
		ID:                productId,
		ID_2:              EcId,
		SlotDate:          startDateFormat,
		SlotDate_2:        endDateFormat,
		AppointmentTypeID: appointmentTypeId,
	})
	if err != nil {
		log.Info("[Odoo - Connector - GetTestDriveTimeSlot Standard] Error: ", err.Error())
		return list, err
	}

	for _, row := range slotTimeRow {
		stringIdx := utils.InterfaceToString(row.Combination)
		if _, ok := mapping[stringIdx]; !ok {
			mapping[stringIdx] = &model.SlotTimeResponses{
				LocationID:          int(row.EcID),
				LocationName:        row.EcName,
				ProductID:           int(row.ProductID),
				ProductName:         row.ProductName,
				AppointmentTypeID:   int(row.AppointmentTypeID),
				AppointmentTypeName: row.AppointmentTypeName,
				Date:                row.BookingDate,
			}
		}

		mapping[stringIdx].TimeSlots = append(mapping[stringIdx].TimeSlots, model.TimeSlot{
			StartTime:    row.Stime,
			EndTime:      row.Etime,
			IsoStartTime: row.StartTimeIso,
			IsoEndTime:   row.EndTimeIso,
			Available:    fmt.Sprintf("%d", row.Jml),
		})
	}

	for _, row := range mapping {
		list = append(list, *row)
	}

	return list, err
}

func (r *repository) timeSlotOnWheels(productId int32, EcId int32, startDateFormat time.Time, endDateFormat time.Time, appointmentTypeId int32) (list []model.SlotTimeResponses, err error) {
	var (
		mapping = make(map[string]*model.SlotTimeResponses)
	)

	slotTimeRow, err := r.qry.GetSlotTimeOnwheels(context.Background(), &query.GetSlotTimeOnwheelsParams{
		ID:         productId,
		ID_2:       EcId,
		SlotDate:   startDateFormat,
		SlotDate_2: endDateFormat,
	})
	if err != nil {
		log.Info("[Odoo - Connector - GetTestDriveTimeSlot OnWheels] Error: ", err.Error())
		return list, err
	}

	for _, row := range slotTimeRow {
		stringIdx := utils.InterfaceToString(row.Combination)
		if _, ok := mapping[stringIdx]; !ok {
			mapping[stringIdx] = &model.SlotTimeResponses{
				LocationID:          int(row.EcID),
				LocationName:        row.EcName,
				ProductID:           int(row.ProductID),
				ProductName:         row.ProductName,
				AppointmentTypeID:   int(row.AppointmentTypeID),
				AppointmentTypeName: row.AppointmentTypeName,
				Date:                row.BookingDate,
			}
		}

		mapping[stringIdx].TimeSlots = append(mapping[stringIdx].TimeSlots, model.TimeSlot{
			StartTime:    row.Stime,
			EndTime:      row.Etime,
			IsoStartTime: row.StartTimeIso,
			IsoEndTime:   row.EndTimeIso,
			Available:    fmt.Sprintf("%d", row.Jml),
		})
	}

	for _, row := range mapping {
		list = append(list, *row)
	}

	return list, err
}

func (r *repository) GetProductStock(purchaseParams model.PurchaseParams) (result model.PurchaseStock, err error) {
	defer log.Info("[Odoo - Connector - GetProductStock] End")

	log.Info("[Odoo - Connector - GetProductStock] Start")
	var (
		colorVariantId, batteryVariantId, wheelVariantId, mirrorVarianId string = "0", "0", "0", "0"
		vehicleCode                                                      string = ""
		uId                                                              int    = 0
	)

	dealerId, _ := utils.StringToInt(purchaseParams.DealerID)
	for _, order := range purchaseParams.Orders {
		vehicleCode = order.ProductCode
		for _, attr := range order.Attributes {
			switch attr.AttributeID {
			case "4":
				mirrorVarianId = attr.VariantID
			case "5":
				wheelVariantId = attr.VariantID
			case "10":
				colorVariantId = attr.VariantID
			case "11":
				batteryVariantId = attr.VariantID
			}
		}
	}

	log.Info(fmt.Sprintf("[Odoo - Connector - GetProductStock] Get Data Product Stock dealer: %d, product: %s, colorId: %s, Battrery: %s, Mirror: %s, Wheel: %s",
		dealerId, vehicleCode, colorVariantId, batteryVariantId, mirrorVarianId, wheelVariantId,
	))
	getProductSoResult, err := r.qry.GetProductStock(context.Background(), &query.GetProductStockParams{
		FnGetProductStock:   dealerId,
		FnGetProductStock_2: uId,
		FnGetProductStock_3: vehicleCode,
		FnGetProductStock_4: colorVariantId,
		FnGetProductStock_5: batteryVariantId,
		FnGetProductStock_6: mirrorVarianId,
		FnGetProductStock_7: wheelVariantId,
	})

	if err != nil {
		return result, err
	}

	// Sample Output : 0|Searching Product Succesfully A11113|104|33000000|1|EV-V Sporty Single Battery|4
	productSoResult := strings.Split(getProductSoResult, utils.ConnectorOdooSeparator)

	result.Code = productSoResult[0]
	result.Message = productSoResult[1]

	// If Code == "1" then return
	if productSoResult[0] == "1" {
		return result, err
	}

	result = model.PurchaseStock{
		ProductCode:  productSoResult[8],
		Qty:          productSoResult[7],
		ProductPrice: productSoResult[3],
	}

	return result, err
}

func (r *repository) GetBookingServiceList(uID string) (list []model.ServiceBookingResponse, err error) {
	log.Info(fmt.Sprintf("[Odoo - Connector - GetBookingServiceList] Get Data Booking Service : \n%s\n", uID))
	stringResult, err := r.qry.GetBookingServiceList(context.Background(), uID)
	if err != nil {
		log.Info(fmt.Sprintf("[Odoo - Connector - GetBookingServiceList] Error : \n%s\n", err.Error()))
		return list, err
	}

	if stringResult != "" {
		json.Unmarshal([]byte(stringResult), &list)
	}

	return list, err
}

func (r *repository) SetPreOrderConfirmation(purchaseParams model.PurchaseParams) (result model.PreOrderResponse, err error) {
	defer log.Info("[Odoo - Connector - SetPreOrderConfirmation] End")
	log.Info("[Odoo - Connector - SetPreOrderConfirmation] Start")

	orderId, _ := utils.StringToInt(purchaseParams.SalesOrderID)

	if orderId == 0 {
		preOrderParam := &model.PreOrderParams{
			PartnerID: purchaseParams.CustomerID,
			CompanyID: purchaseParams.DealerID,
		}

		for _, order := range purchaseParams.Orders {
			preOrderDetails := model.PreOrderDetails{
				ProductCode: order.ProductCode,
				Qty:         string(order.Qty),
				Attributes:  order.Attributes,
			}
			preOrderParam.Orders = append(preOrderParam.Orders, preOrderDetails)
		}

		preOrderParamJSON, _ := json.Marshal(preOrderParam)
		var preOrderParamJSONMap map[string]interface{}
		json.Unmarshal(preOrderParamJSON, &preOrderParamJSONMap)

		log.Info("[Odoo - Connector - SetPreOrderConfirmation] Execute X.Booking.Fee - CreatdealerIde")
		log.Info(fmt.Sprintf("[Odoo - Connector - SetPreOrderConfirmation] Execute create_booking_fee with Params 1: \n%#v\n", preOrderParamJSONMap))
		bookingFeeResponse, err := r.rpc.ExecuteKw("create_booking_fee", "x.booking.fee", []interface{}{preOrderParamJSONMap}, nil)
		if err != nil {
			return result, err
		}

		// jsonStr, err := json.Marshal(bookingFeeResponse)
		// if err != nil {
		// 	log.Error(err)
		// }

		// preOrderResult := new(model.PreOrderResponse)
		// if err := json.Unmarshal(jsonStr, &preOrderResult); err != nil {
		// 	log.Error(err)
		// }
		preOrderResult := jMarshal(bookingFeeResponse)

		return preOrderResult, nil
	}

	log.Info("[Odoo - Connector - SetPreOrderConfirmation] Get PreOrder Detail By BookingFeeId : ", orderId)
	params := map[string]interface{}{
		"booking_fee_id": orderId,
	}
	viewResponse, err := r.rpc.ExecuteKw("view_booking_fee", "x.booking.fee", []interface{}{params}, nil)
	// jsonStr, err := json.Marshal(viewResponse)
	// if err != nil {
	// 	log.Error(err)
	// }

	// preOrderResult := new(model.PreOrderResponse)
	// if err := json.Unmarshal(jsonStr, &preOrderResult); err != nil {
	// 	log.Error(err)
	// }
	preOrderResult := jMarshal(viewResponse)

	return preOrderResult, nil
}

func (r *repository) SetPreOrderPaymentMethod(salesOrderId int32, paymentMethodCode string) (result model.PreOrderResponse, err error) {
	defer log.Info("[Odoo - Connector - SetPreOrderPaymentMethod] End")
	log.Info("[Odoo - Connector - SetPreOrderPaymentMethod] Start")

	params := map[string]interface{}{
		"booking_fee_id": salesOrderId,
		"product_code":   paymentMethodCode,
	}
	log.Info("[Odoo - Connector - SetPreOrderConfirmation] Set PaymentMethod for BookingFeeID : ", salesOrderId)
	paymentResponse, err := r.rpc.ExecuteKw("set_payment_method", "x.booking.fee", []interface{}{params}, nil)
	// jsonStr, err := json.Marshal(paymentResponse)
	// if err != nil {
	// 	log.Error(err)
	// }

	// preOrderResult := new(model.PreOrderResponse)
	// if err := json.Unmarshal(jsonStr, &preOrderResult); err != nil {
	// 	log.Error(err)
	// }
	preOrderResult := jMarshal(paymentResponse)

	return preOrderResult, nil
}

func (r *repository) ResetPreOrderPaymentMethod(salesOrderId int32) (result model.PreOrderResponse, err error) {
	defer log.Info("[Odoo - Connector - ResetPreOrderPaymentMethod] End")
	log.Info("[Odoo - Connector - ResetPreOrderPaymentMethod] Start")

	params := map[string]interface{}{
		"booking_fee_id": salesOrderId,
	}
	log.Info("[Odoo - Connector - SetPreOrderConfirmation] Reset PaymentMethod for BookingFeeID : ", salesOrderId)
	paymentResponse, err := r.rpc.ExecuteKw("reset_payment_method", "x.booking.fee", []interface{}{params}, nil)
	// jsonStr, err := json.Marshal(paymentResponse)
	// if err != nil {
	// 	log.Error(err)
	// }

	// preOrderResult := new(model.PreOrderResponse)
	// if err := json.Unmarshal(jsonStr, &preOrderResult); err != nil {
	// 	log.Error(err)
	// }
	preOrderResult := jMarshal(paymentResponse)

	return preOrderResult, nil
}

func (r *repository) PreOrderPaymentConfirm(salesOrderId int32) (result model.PreOrderResponse, err error) {
	defer log.Info("[Odoo - Connector - PreOrderPaymentConfirm] End")
	log.Info("[Odoo - Connector - PreOrderPaymentConfirm] Start")

	params := map[string]interface{}{
		"booking_fee_id": salesOrderId,
	}
	log.Info("[Odoo - Connector - PreOrderPaymentConfirm] Set PaymentConfirm for BookingFeeID : ", salesOrderId)
	paymentResponse, err := r.rpc.ExecuteKw("confirm_booking_fee", "x.booking.fee", []interface{}{params}, nil)
	// jsonStr, err := json.Marshal(paymentResponse)
	// if err != nil {
	// 	log.Error(err)
	// }

	// preOrderResult := new(model.PreOrderResponse)
	// if err := json.Unmarshal(jsonStr, &preOrderResult); err != nil {
	// 	log.Error(err)
	// }
	preOrderResult := jMarshal(paymentResponse)

	return preOrderResult, nil
}
